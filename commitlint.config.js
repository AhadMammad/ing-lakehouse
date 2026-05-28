module.exports = {
  extends: ['@commitlint/config-conventional'],
  rules: {
    'type-enum': [
      2,
      'always',
      ['feat', 'fix', 'chore', 'docs', 'refactor', 'perf', 'test', 'build', 'ci', 'revert'],
    ],
    'scope-enum': [
      1,
      'always',
      ['data-generator', 'etl-app', 'airflow', 'trino', 'iceberg', 'nessie', 'rustfs', 'kafka', 'jupyter', 'infra', 'docs', 'ci', 'deps'],
    ],
    'subject-case': [0],
  },
};
