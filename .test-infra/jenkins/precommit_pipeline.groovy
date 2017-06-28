#!groovy

stage('Build') {
    step {
        node {
            sh(script: "echo ${ghprbPullID}")
        }
    }
    // build job: 'beam_PreCommit_Build', parameters: [string(name: 'sha1', value: )]
}