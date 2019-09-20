#!/usr/bin/env bash
echo "Printing 'docker service ls | grep dtz_master':"
docker service ls | grep dtz_master
echo ""
echo "Printing 'docker service ps dtz_master':"
docker service ps dtz_master_controller_dtz_master_controller
