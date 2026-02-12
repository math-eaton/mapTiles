#!/bin/sh

aria2c \
    --max-concurrent-downloads=16 \
    --max-connection-per-server=16 \
    --split=16 \
    --min-split-size=10M \
    --allow-overwrite=true \
    -o terrain.pmtiles \
    https://download.mapterhorn.com/planet.pmtiles


    pmtiles extract \
  --bbox=-18.8,-35.4,51.8,37.5 \
  https://download.mapterhorn.com/planet.pmtiles \
  terrain.pmtiles