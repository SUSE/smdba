#!/bin/bash

# Get Python exec
function get_python_exec () {
  for pyexe in python3.7 python3.6 python3; do
    if hash $pyexe 2>/dev/null; then
      echo $pyexe
      break
    fi
  done
}


# Create gitignore and add venv
function add_to_git_ignore () {
  REF=$1
  GIGN="./.gitignore"
  if [ ! -f "$GIGN" ]; then
    echo -e > $GIGN;
  fi

  # If reference is already there
  if [ -z "$(cat $GIGN | grep $REF)" ]; then
    echo $REF >> $GIGN
  fi
}

# Create virtual environment.
function create_venv() {
  VENV=".smdba-venv"
  if [ ! -d $VENV ]; then
    PYEXE=$(get_python_exec)
    $PYEXE -m venv $VENV
  fi
  echo $VENV
}

# Install deps
function install_deps() {
  pip install --upgrade pip
  pip install pytest
  pip install pylint
  pip install mypy
}


venv=$(create_venv)
add_to_git_ignore $venv
source "$venv/bin/activate"
install_deps
ln -s "$(readlink -f .)/src/smdba" $(dirname $(python -c "import pytest as _; print(_.__file__)")) 2>/dev/null
pytest -sv tests

cat <<EOF

==================================== MyPy Tests [start] ===================================
EOF
mypy --config-file ./.mypyrc --strict src/smdba/

if [ $? -ne 0 ]; then
    cat <<EOF

=================================== MyPy Tests [finish] ===================================
There are still some issues with static run. Unless this is oraclegate.py, please fix them.

EOF
else
    cat <<EOF

Type check seems OK.
EOF
fi
