# Rolling zarr creator

Creates a rolling zarr by taking the Met Office AWS earth data in NetCDF and creating individual zarr chunks from it. This step can happen in parallel with many files.
Separately update the metadata in the zarr(s) to reflect the new size/shape as data is added. This is the zarr-roller job.
Also update the zarrs used by xarray to interpret the coordinates of the data.

Uses an 'offset map' to create the rolling behaviour this is done by a using the offset map as the zarr store which interprets meta data and applies and offset to the requested chunk. i.e. you ask for chunk 1.0.0.0 but the metadata says there is an offset of 2.0.0.0 so you get chunk 3.0.0.0. This behaviour creates a rolling view on the data with out having to re-write chunks. e.g. over time chunk 3.0.0 on 'disk' is accessed first as '3.0.0' then as meta is updated '2.0.0' then '1.0.0' then '0.0.0' and then it 'drops of' and is no longer accessible (regardless of if it's on disk or not) 

The `offset map` also attempts to use s3 tags to tag the s3 objects that should be periodically removed. These should be the data chunks which should be set to expire from s3 once they have rolled outside the rolling window.

Job `zarr-roller` ingests messages (which are metadata and paths to an NetCDF object) off a queue and process one message a time re-chunking and writing the zarr chunks.
Job `zarr-roller-meta-update` updates the metadata periodically pushing forward the rolling window.

At their heart both jobs use jupyter notebooks stored in s3, these are run with papermill, the results/logs are written back to s3.
The notebooks used are attached included in the repo for illustration but the actual deployment currently used notebooks on s3 and so the versions here may or may not stay in sync with what actually runs. They should be included in the version control properly but that is a TODO.


## install

Install on kubernetes  with `helm` like:

create/update:

```shell
helm upgrade --install --namespace=zarr-roller zarr-roller zarr-roller
```

delete:
```
kubectl -n zarr-roller delete jobs zarr-roller
```

trigger the jobs now:

```shell
kubectl -n zarr-roller create job --from=cronjob/zarr-roller zarr-roller-`date +%Y-%m-%dt%H.%M.%S`
kubectl -n zarr-roller create job --from=cronjob/zarr-roller-meta-update zarr-roller-meta-update-`date +%Y-%m-%dt%H.%M.%S`
````