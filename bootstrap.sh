#!/usr/bin/env bash

cat <<EOF > shell
#!/usr/bin/env bash


if [ "\$1" == "" ]; then
  export COFFEA_IMAGE=coffeateam/coffea-dask-cc7:latest
else
  export COFFEA_IMAGE=\$1
fi

grep -v '^include' /etc/condor/config.d/00hep_wisc.config > .condor_config

EXTERNAL_BIND=\${PWD} singularity exec -B \${PWD}:/srv -B /etc/condor -B /scratch -B /afs --pwd /srv \\
  /cvmfs/unpacked.cern.ch/registry.hub.docker.com/\${COFFEA_IMAGE} \\
  /bin/bash --rcfile /srv/.bashrc
EOF

cat <<EOF > .bashrc
LPCJQ_VERSION="0.2.10"
install_env() {
  set -e
  echo "Installing shallow virtual environment in \$PWD/.env..."
  python -m venv --without-pip --system-site-packages .env
  echo "Virtual environment created..."
  unlink .env/lib64  # HTCondor can't transfer symlink to directory and it appears optional
  # work around issues copying CVMFS xattr when copying to tmpdir
  export TMPDIR=\$(mktemp -d -p .)
  echo "Created Temporary directory..."
  .env/bin/python -m ipykernel install --user
  rm -rf \$TMPDIR && unset TMPDIR
  echo "HTCondor workaround executed successfully..."
  .env/bin/python -m pip install -q git+https://github.com/CoffeaTeam/lpcjobqueue.git@v\${LPCJQ_VERSION}
  echo "LPCJobqueue installed..."
  echo "done."
}

export JUPYTER_PATH=/srv/.jupyter
export JUPYTER_RUNTIME_DIR=/srv/.local/share/jupyter/runtime
export JUPYTER_DATA_DIR=/srv/.local/share/jupyter
export IPYTHONDIR=/srv/.ipython

[[ -d .env ]] || install_env
source .env/bin/activate
alias pip="python -m pip"
pip show lpcjobqueue 2>/dev/null | grep -q "Version: \${LPCJQ_VERSION}" || pip install -q git+https://github.com/CoffeaTeam/lpcjobqueue.git@v\${LPCJQ_VERSION}
EOF

chmod u+x shell .bashrc
echo "Wrote shell and .bashrc to current directory. You can delete this file. Run ./shell to start the singularity shell"
