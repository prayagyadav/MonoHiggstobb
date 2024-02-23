#!/usr/bin/bash
echo "CONDOR BATCH SUBMIT"
echo " "
echo "TTToSemiLeptonic"
echo "starting..."
export COFFEA_IMAGE=coffeateam/coffea-dask-cc7:latest-py3.10
export EXTERNAL_BIND=../

mkdir -p ./logs

nohup singularity exec -B ../:/srv -B /etc/condor -B /scratch -B /afs --pwd /srv /cvmfs/unpacked.cern.ch/registry.hub.docker.com/${COFFEA_IMAGE} python runner_Top.py -k TTToSemiLeptonic -e condor -c 500000 -cat resolved -lepton e --begin 1 --end 50 -w 16 > ./logs/log_TTToSemiLeptonic_1_to_50.txt 2>&1 &
echo "TTToSemiLeptonic_1_to_50 submitted with job id $!" | tee -a ./logs/jobids.txt
sleep 1
nohup singularity exec -B ../:/srv -B /etc/condor -B /scratch -B /afs --pwd /srv /cvmfs/unpacked.cern.ch/registry.hub.docker.com/${COFFEA_IMAGE} python runner_Top.py -k TTToSemiLeptonic -e condor -c 500000 -cat resolved -lepton e --begin 51 --end 100 -w 16 > ./logs/log_TTToSemiLeptonic_51_to_100.txt 2>&1 &
echo "TTToSemiLeptonic_51_to_100 submitted with job id $!" | tee -a ./logs/jobids.txt
sleep 1
nohup singularity exec -B ../:/srv -B /etc/condor -B /scratch -B /afs --pwd /srv /cvmfs/unpacked.cern.ch/registry.hub.docker.com/${COFFEA_IMAGE} python runner_Top.py -k TTToSemiLeptonic -e condor -c 500000 -cat resolved -lepton e --begin 101 --end 150 -w 16 > ./logs/log_TTToSemiLeptonic_101_to_150.txt 2>&1 &
echo "TTToSemiLeptonic_101_to_150 submitted with job id $!" | tee -a ./logs/jobids.txt
sleep 1
nohup singularity exec -B ../:/srv -B /etc/condor -B /scratch -B /afs --pwd /srv /cvmfs/unpacked.cern.ch/registry.hub.docker.com/${COFFEA_IMAGE} python runner_Top.py -k TTToSemiLeptonic -e condor -c 500000 -cat resolved -lepton e --begin 151 --end 200 -w 16 > ./logs/log_TTToSemiLeptonic_151_to_200.txt 2>&1 &
echo "TTToSemiLeptonic_151_to_200 submitted with job id $!" | tee -a ./logs/jobids.txt
sleep 1
nohup singularity exec -B ../:/srv -B /etc/condor -B /scratch -B /afs --pwd /srv /cvmfs/unpacked.cern.ch/registry.hub.docker.com/${COFFEA_IMAGE} python runner_Top.py -k TTToSemiLeptonic -e condor -c 500000 -cat resolved -lepton e --begin 201 --end 250 -w 16 > ./logs/log_TTToSemiLeptonic_201_to_250.txt 2>&1 &
echo "TTToSemiLeptonic_201_to_250 submitted with job id $!" | tee -a ./logs/jobids.txt
sleep 1
nohup singularity exec -B ../:/srv -B /etc/condor -B /scratch -B /afs --pwd /srv /cvmfs/unpacked.cern.ch/registry.hub.docker.com/${COFFEA_IMAGE} python runner_Top.py -k TTToSemiLeptonic -e condor -c 500000 -cat resolved -lepton e --begin 251 --end 300 -w 16 > ./logs/log_TTToSemiLeptonic_251_to_300.txt 2>&1 &
echo "TTToSemiLeptonic_251_to_300 submitted with job id $!" | tee -a ./logs/jobids.txt
sleep 1
nohup singularity exec -B ../:/srv -B /etc/condor -B /scratch -B /afs --pwd /srv /cvmfs/unpacked.cern.ch/registry.hub.docker.com/${COFFEA_IMAGE} python runner_Top.py -k TTToSemiLeptonic -e condor -c 500000 -cat resolved -lepton e --begin 301 --end 350 -w 16 > ./logs/log_TTToSemiLeptonic_301_to_350.txt 2>&1 &
echo "TTToSemiLeptonic_301_to_350 submitted with job id $!" | tee -a ./logs/jobids.txt
sleep 1
nohup singularity exec -B ../:/srv -B /etc/condor -B /scratch -B /afs --pwd /srv /cvmfs/unpacked.cern.ch/registry.hub.docker.com/${COFFEA_IMAGE} python runner_Top.py -k TTToSemiLeptonic -e condor -c 500000 -cat resolved -lepton e --begin 351 --end 389 -w 16 > ./logs/log_TTToSemiLeptonic_351_to_389.txt 2>&1 &
echo "TTToSemiLeptonic_351_to_389 submitted with job id $!" | tee -a ./logs/jobids.txt
sleep 1

echo "The End"
