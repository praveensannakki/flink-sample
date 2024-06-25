echo "killing flink job"
YARN_APP_ID=$(yarn application -list 2>&1 | grep 'application_' | head -1 | cut -d$'\t' -f1)
FLINK_JOB_ID=$(flink list -t yarn-session -Dyarn.application.id=${YARN_APP_ID} 2>&1 | grep '(RUNNING' | head -1 | cut -d: -f4 | sed 's/ *//g')

echo "yarn app id=${YARN_APP_ID}"
echo "flink job id=${FLINK_JOB_ID}"

if [[ X${YARN_APP_ID} == X'' || X${FLINK_JOB_ID} == X'' ]]; then
  echo 'Flink app is not running on this cluster, exiting...'
else
  echo "flink stop -t yarn-session -Dyarn.applicaiton.id=${YARN_APP_ID} ${FLINK_JOB_ID}"
  flink stop -t yarn-session -Dyarn.applicaiton.id=${YARN_APP_ID} ${FLINK_JOB_ID}
  if [[ $? -ne 0 ]]; then
    echo "Failed to stop the flink app ${FLINK_JOB_ID} with savepoint, so cancelling it.."
    echo "flink cancel -t yarn-session -Dyarn.applicaiton.id=${YARN_APP_ID} ${FLINK_JOB_ID}"
    flink stop -t yarn-session -Dyarn.applicaiton.id=${YARN_APP_ID} ${FLINK_JOB_ID}
  fi
fi