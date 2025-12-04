import json
import subprocess

from flask import Flask, jsonify, request

app = Flask(__name__)


@app.route("/api/run-analysis", methods=["POST"])
def run_analysis():
    payload = request.get_json(force=True)
    platform = payload.get("platform")
    target_date = payload.get("date")
    historical_weeks = payload.get("historical_weeks", 4)

    if not platform or not target_date:
        return jsonify({"status": "error", "message": "platform and date are required"}), 400

    cmd = [
        "spark-submit",
        "etl_job.py",
        "--platform",
        platform,
        "--target_date",
        target_date,
        "--weeks_back",
        str(historical_weeks),
    ]

    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        return jsonify({"status": "error", "message": result.stderr}), 500

    stdout_lines = [line for line in result.stdout.splitlines() if line.strip()]
    last_line = stdout_lines[-1] if stdout_lines else ""
    try:
        spark_output = json.loads(last_line)
    except json.JSONDecodeError:
        return jsonify({"status": "error", "message": "Failed to parse Spark output"}), 500

    response = {"status": "success", "results": {platform: spark_output}}
    return jsonify(response)


def create_app():
    return app


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
