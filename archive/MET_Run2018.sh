#!/usr/bin/bash
echo "CONDOR BATCH SUBMIT"
echo " "
echo "MET_Run2018"
echo "starting..."
export COFFEA_IMAGE=coffeateam/coffea-dask-cc7:latest-py3.10
export EXTERNAL_BIND=../

mkdir -p ./logs

nohup singularity exec -B ../:/srv -B /etc/condor -B /scratch -B /afs --pwd /srv /cvmfs/unpacked.cern.ch/registry.hub.docker.com/${COFFEA_IMAGE} python runner_Top.py -k MET_Run2018 -e condor -c 500000 -cat resolved -lepton e --begin 1 --end 39 -w 16 > ./logs/log_MET_Run2018A.txt 2>&1 &
echo "MET_Run2018A submitted with job id $!" | tee -a ./logs/jobids.txt
sleep 30
nohup singularity exec -B ../:/srv -B /etc/condor -B /scratch -B /afs --pwd /srv /cvmfs/unpacked.cern.ch/registry.hub.docker.com/${COFFEA_IMAGE} python runner_Top.py -k MET_Run2018 -e condor -c 500000 -cat resolved -lepton e --begin 40 --end 61 -w 16 > ./logs/log_MET_Run2018B.txt 2>&1 &
echo "MET_Run2018B submitted with job id $!" | tee -a ./logs/jobids.txt
sleep 30
nohup singularity exec -B ../:/srv -B /etc/condor -B /scratch -B /afs --pwd /srv /cvmfs/unpacked.cern.ch/registry.hub.docker.com/${COFFEA_IMAGE} python runner_Top.py -k MET_Run2018 -e condor -c 500000 -cat resolved -lepton e --begin 62 --end 85 -w 16 > ./logs/log_MET_Run2018C.txt 2>&1 &
echo "MET_Run2018C submitted with job id $!" | tee -a ./logs/jobids.txt
sleep 30
nohup singularity exec -B ../:/srv -B /etc/condor -B /scratch -B /afs --pwd /srv /cvmfs/unpacked.cern.ch/registry.hub.docker.com/${COFFEA_IMAGE} python runner_Top.py -k MET_Run2018 -e condor -c 500000 -cat resolved -lepton e --begin 86 --end 206 -w 16 > ./logs/log_MET_Run2018D.txt 2>&1 &
echo "MET_Run2018D submitted with job id $!" | tee -a ./logs/jobids.txt
sleep 30

echo "The End"
