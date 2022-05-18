pipelineJob('deploy-branch-to-k8s-gha') {

  disabled(ENVIRONMENT != 'prod')
  displayName 'Deploy branch to K8s (via GitHub)'
  description 'Deploys branch to Kubernetes using a GitHub Actions Workflow.'

  definition {
    cps {
      script(readFileFromWorkspace('.ci/pipelines/deploy_k8s_branches_gha.groovy'))
      sandbox()
    }
  }

  parameters {
    booleanParam('DRY_RUN', false, 'Enable dry-run mode.')
    stringParam('DOCKER_TAG', 'latest', 'Docker tag to deploy.')
    stringParam('BRANCH', 'master', 'Tasklist branch being deployed. Determines the name of your deployment; needs to be a DNS-compatible name.')
    stringParam('REF', 'master', 'Git ref of the workflow to trigger.')
  }
}
