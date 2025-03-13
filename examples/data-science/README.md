# Trivial data science example

This example program parses and analyzes the NYC air quality dataset.
It is used to demonstrate the abilities of the FUSE filesystem to stream data, instead of downloading a dataset in advance.

## How to use:

Terminal 1:
```
umount mnt
go run ../../cmd/asset-fuse mount --manifest ../manifests/manifest_datasets.json --log_level=debug mnt
```

Terminal 2:
```
go run ./air-quality.go mnt/air-quality/Air_Quality.csv
```

Compare to local execution (you need to download the file in advance):

```
wget -O /tmp/Air_Quality.csv https://data.cityofnewyork.us/api/views/c3uy-2p5r/rows.csv?accessType=DOWNLOAD
go run ./air-quality.go /tmp/Air_Quality.csv
```

When your done, simply unmount the filesystem with `umount mnt`
