# Lite GCSFUSE Demo

This is a lite demo verion of [Cloud Storage FUSE](https://cloud.google.com/storage/docs/gcs-fuse) (gcsfuse).

It demos how to transfer data between GCS bucket via a file as the interface. To improve performance, the data doesn't stage on disk.

## Run

```bash
# clone this repo

# make sure the bucket exist
go run main.go --bucket=<bucket name> --object=<object name>

# a FUSE filesystem will be mounted on /tmp/y by default,
# you can change it by --mount flag

# there is only one file: /tmp/y/gcs_file

# use another session, do some read/write

# write data. The data will be redirected to the GCS object specified above
echo "lite fuse demo" > /tmp/y/gcs_file

# read data. The data is from that GCS object
cat /tmp/y/gcs_file
```
