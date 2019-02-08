#!/usr/bin/env bash
docker-compose build
docker-compose push || true
docker stack deploy --compose-file docker-compose.yml dtz_master_controller
