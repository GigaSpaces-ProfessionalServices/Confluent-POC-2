#!/bin/bash
export GS_HOME=/home/vagrant/gigaspaces-insightedge-enterprise-15.2.0
$GS_HOME/bin/gs.sh space deploy --partitions=2 wordcount-store
