rclone copyto tilejson.json grid3-tiles-rclone:grid3-tiles/tilejson.json --progress --s3-no-check-bucket --s3-chunk-size=256M --header-upload "Content-Type: application/json"


"Content-Type: application/vnd.pmtiles"