#/bin/bash
chmod +x ./scripts/* && \
. scripts/env.sh && . /opt/venv/bin/activate && \
pip install -r dev-requirements.txt && \
git config --global --add safe.directory ${PWD} && \
pre-commit install