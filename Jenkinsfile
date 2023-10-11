@Library('build-library') _
import ge.energy.grid.*

pipeline {
    agent {
        label 'ods-dind'
    }

    stages {
        stage('Build Image') {
            steps {
	    sh "docker build -tag d ."
            }
        }
    }
}