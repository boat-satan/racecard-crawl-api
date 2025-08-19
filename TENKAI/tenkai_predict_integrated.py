name: Predict TENKAI (tansyo + kimarite)

on:
  workflow_dispatch:
    inputs:
      date:
        description: '対象日 (YYYYMMDD)'
        required: true
      pid:
        description: '場コード pid (例: 02)'
        required: true
      race:
        description: 'レース (例: 2R) 空/ALL なら全R'
        required: false
        default: ''
      model_date:
        description: 'モデル日付（空=最新を自動採用）'
        required: false
        default: ''

permissions:
  contents: write

concurrency:
  group: tenkai-predict-${{ github.ref }}
  cancel-in-progress: true

jobs:
  predict:
    runs-on: ubuntu-latest
    steps:
      - name: Checkout
        uses: actions/checkout@v4

      - name: Setup Python
        uses: actions/setup-python@v5
        with:
          python-version: '3.11'

      - name: Install dependencies
        run: |
          python -m pip install --upgrade pip
          pip install pandas scikit-learn joblib numpy scipy lightgbm

      - name: Ensure TENKAI package
        run: |
          mkdir -p TENKAI
          [ -f TENKAI/__init__.py ] || printf "" > TENKAI/__init__.py

      - name: Run prediction (tansyo + kimarite)
        shell: bash
        run: |
          set -euo pipefail
          DATE="${{ github.event.inputs.date }}"
          PID="${{ github.event.inputs.pid }}"
          RACE="${{ github.event.inputs.race }}"
          MODEL_DATE="${{ github.event.inputs.model_date }}"

          RACE_OPT=""
          if [ -n "${RACE}" ] && [ "${RACE}" != "ALL" ]; then
            RACE_OPT="--race ${RACE}"
          fi

          MODEL_OPT=""
          if [ -n "${MODEL_DATE}" ]; then
            MODEL_OPT="--model_date ${MODEL_DATE}"
          fi

          echo ">>> run predict: ${DATE} ${PID} ${RACE_OPT} ${MODEL_OPT}"
          PYTHONPATH="." python TENKAI/tenkai_predict_integrated.py --date "${DATE}" --pid "${PID}" ${RACE_OPT} ${MODEL_OPT}

      - name: List predictions
        run: |
          echo "---- generated files ----"
          ls -R TENKAI/predictions/v1 || true

      - name: Commit predictions
        shell: bash
        run: |
          set -euo pipefail
          DATE="${{ github.event.inputs.date }}"
          PID="${{ github.event.inputs.pid }}"
          RACE="${{ github.event.inputs.race }}"
          ADD_PATH="TENKAI/predictions/v1/${DATE}/${PID}/**/*.csv"

          git config user.name  "github-actions[bot]"
          git config user.email "github-actions[bot]@users.noreply.github.com"
          git fetch origin
          git pull --rebase origin "${GITHUB_REF_NAME:-main}" || true
          git add ${ADD_PATH} || true
          git diff --cached --quiet || git commit -m "TENKAI predictions: ${DATE} pid=${PID} race=${RACE:-ALL}"
          git push || true

      - name: Upload predictions artifact
        uses: actions/upload-artifact@v4
        with:
          name: tenkai-predict-${{ github.event.inputs.date }}-${{ github.event.inputs.pid }}${{ github.event.inputs.race && format('-{0}', github.event.inputs.race) || '' }}
          path: TENKAI/predictions/v1/${{ github.event.inputs.date }}/${{ github.event.inputs.pid }}/**/*.csv
          if-no-files-found: warn
