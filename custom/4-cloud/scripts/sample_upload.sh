rclone copyto tilejson.json grid3-tiles-rclone:grid3-tiles/tilejson.json --progress --s3-no-check-bucket --s3-chunk-size=256M --header-upload "Content-Type: application/vnd.pmtiles"
rclone copyto buildings.pmtiles grid3-tiles-rclone:grid3-tiles/buildings.pmtiles --progress --s3-no-check-bucket --s3-chunk-size=256M --header-upload "Content-Type: application/vnd.pmtiles"
rclone copyto base.pmtiles grid3-tiles-rclone:grid3-tiles/base.pmtiles --progress --s3-no-check-bucket --s3-chunk-size=256M --header-upload "Content-Type: application/vnd.pmtiles"



"Content-Type: application/vnd.pmtiles"
Content-Type: application/json