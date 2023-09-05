use anyhow::Result;
use carla::{
    client::Client,
    prelude::*,
    rpc::EpisodeSettings,
    sensor::data::{LidarDetection, LidarMeasurement},
};
use clap::Parser;
use itertools::{izip, Itertools};
use na::coordinates::XYZ;
use nalgebra as na;
use pcd_rs::{DataKind, PcdSerialize};
use rayon::prelude::*;
use std::{fs, path::PathBuf, thread, thread::spawn, time::Duration};

#[derive(Parser)]
struct Opts {
    #[clap(short = 'w', long)]
    pub world: Option<String>,

    #[clap(short = 'd', long, default_value = "10.0")]
    pub sampling_distance: f64,

    #[clap(short = 'r', long, default_value = "20.0")]
    pub lidar_range: f64,

    #[clap(short = 'f', long, default_value = "10")]
    pub rotation_frequency: f64,

    #[clap(short = 'p', long, default_value = "90000")]
    pub points_per_second: u32,

    #[clap(short = 'c', long, default_value = "32")]
    pub lidar_channels: u32,

    #[clap(short = 'j', long, default_value = "0")]
    pub jobs: usize,

    pub output_xodr_file: PathBuf,
    pub output_pcd_file: PathBuf,
}

fn main() -> Result<()> {
    let opts = Opts::parse();

    let max_workers = thread::available_parallelism()?.get();
    let n_workers = match opts.jobs {
        0 => max_workers,
        jobs => jobs.min(max_workers),
    };

    let world_name = opts.world;
    let mut client = Client::default();
    client.set_timeout(Duration::from_secs(5));

    let mut world = match &world_name {
        Some(name) => client.load_world(name),
        None => client.world(),
    };

    // Enter synchronous mode
    world.apply_settings(
        &EpisodeSettings {
            synchronous_mode: true,
            fixed_delta_seconds: Some(0.05),
            ..world.settings()
        },
        Duration::ZERO,
    );

    // Write the .xodr file
    let map = world.map();
    let opendrive_text = map.to_open_drive();
    fs::write(opts.output_xodr_file, opendrive_text)?;

    let waypoints: Vec<_> = map
        .generate_waypoints(opts.sampling_distance)
        .iter()
        .collect();
    let n_waypoints = waypoints.len();

    // Spawn sensors in the simulator
    let (finish_tx, finish_rx) = flume::bounded(n_workers * 2); // used to mark a completion of a job
    let (measure_tx, measure_rx) = flume::bounded(n_workers * 2); // used to collect lidar data

    let sensors: Vec<_> = (0..n_workers)
        .map(|_| -> Result<_> {
            let builder = world
                .actor_builder("sensor.lidar.ray_cast")?
                .set_attribute("channels", &opts.lidar_channels.to_string())?
                .set_attribute("points_per_second", &opts.points_per_second.to_string())?
                .set_attribute("rotation_frequency", &opts.rotation_frequency.to_string())?
                .set_attribute("range", &opts.lidar_range.to_string())?;
            let sensor = builder.spawn_sensor(&na::Isometry3::identity())?;

            let (activate_tx, activate_rx) = flume::bounded::<na::Isometry3<f32>>(1);
            let finish_tx = finish_tx.clone();
            let measure_tx = measure_tx.clone();

            sensor.listen(move |data| {
                if let Ok(tf) = activate_rx.try_recv() {
                    let measure: LidarMeasurement = data.try_into().unwrap();
                    measure_tx.send((tf, measure)).unwrap();
                    finish_tx.send(()).unwrap();
                }
            });

            Ok((sensor, activate_tx))
        })
        .try_collect()?;

    // Start a thread that collects lidar points.
    let collector_handle = spawn(move || {
        let points: Vec<_> = measure_rx
            .into_iter()
            .take(n_waypoints)
            .par_bridge()
            .flat_map(|(tf, measure)| {
                let points: Vec<_> = measure
                    .as_slice()
                    .iter()
                    .map(|det| {
                        let LidarDetection {
                            ref point,
                            intensity,
                        } = *det;
                        let point = tf * point.to_na_point();
                        let XYZ { x, y, z } = *point;
                        Point { x, y, z, intensity }
                    })
                    .collect();
                points
            })
            .collect();
        points
    });

    // Distribute jobs to sensors.
    {
        let mut finish_token_iter = finish_rx.into_iter();

        'waypoint_loop: for chunk in waypoints.chunks(n_workers) {
            // Move each sensor to the desired waypoint.
            for (wp, (sensor, _)) in izip!(chunk, &sensors) {
                sensor.set_transform(&wp.transform());
            }

            // Tick the simulator to ensure the sensor location is
            // updated.
            world.tick();

            // Activate sensors to scan data.
            for (wp, (_, activate_tx)) in izip!(chunk, &sensors) {
                let Ok(()) = activate_tx.send(wp.transform()) else {
                    break 'waypoint_loop;
                };
            }

            // Wait for all sensors to finish.
            let count = (&mut finish_token_iter).take(chunk.len()).count();
            assert_eq!(count, chunk.len());
        }
    }

    // Wait for all points to be collected.
    let points = collector_handle.join().unwrap();

    // Stop sensor listeners.
    for (sensor, _) in sensors {
        sensor.stop();
    }

    // Write to a .pcd file
    {
        let mut writer = pcd_rs::WriterInit {
            width: points.len() as u64,
            height: 1,
            viewpoint: Default::default(),
            data_kind: DataKind::Binary,
            schema: None,
        }
        .create(&opts.output_pcd_file)?;

        for point in points {
            writer.push(&point)?;
        }

        writer.finish()?;
    }

    // Back to asynchronous mode
    world.apply_settings(
        &EpisodeSettings {
            synchronous_mode: false,
            fixed_delta_seconds: None,
            ..world.settings()
        },
        Duration::ZERO,
    );

    Ok(())
}

#[derive(Debug, Clone, PcdSerialize)]
struct Point {
    pub x: f32,
    pub y: f32,
    pub z: f32,
    pub intensity: f32,
}
