def pod_name = 'kavetoolbox'
def pod_label = "${pod_name}-jenkins-slave-${UUID.randomUUID().toString()}"
def jenkinsenv = 'jenkinsenv'

def python_packages = [ 'pip', 'virtualenv', 'tox']

// Little helper to generate a status message for a
// stage. Also, serves as an Froovy method/string
// templating example.
//
// Note that Groovy always returns the last evaluated
// statement.
def status_string(String msg, String result) {
    if (!msg.empty) {
        msg += "\n"
        msg += result
        return msg
    } else {
        return result
    }
}


properties([
        pipelineTriggers([upstream(
                    threshold: 'SUCCESS',
                    upstreamProjects: '../eskapade/master, ../eskapade/split_esroofit'
                )
            ]
        )
    ]
)

podTemplate(
    name: pod_name,
    label: pod_label,
    namespace: 'jenkins',
    containers: [
        containerTemplate(name: 'jnlp',
                          image: 'kave/jnlp-slave-kavetoolbox',
                          workingDir: '/home/jenkins',
                          args: '${computer.jnlpmac} ${computer.name}',
                          envVars: [ envVar(key: 'JENKINS_URL', value: 'http://its-jenkins:8080')
            ]
        )
    ]
) {
    node(pod_label) {

        def status_msg = ''

        stage('Setup') {
            try {
                echo "Creating and setting up Python virtual environment for ${env.JOB_NAME}."

                // Build the shell script to setup and create the
                // virtual environment. This will be executed by
                // Jenkins on the slave.
                def to_execute = "#!/bin/bash\n" +
                    "conda create --name ${jenkinsenv} -y\n" +
                    "source activate jenkinsenv\n" +
                    "conda install pip -y\n"

                python_packages.each {
                    to_execute += "pip install --upgrade ${it}\n"
                }

                // Let's execute the script and capture the output
                // on the slave.
                container(name: 'jnlp', shell: '/bin/bash') {
                    status_msg = sh(returnStdout: true, script: to_execute).trim()
                }

                echo status_string(status_msg, 'SUCCESS')
                currentBuild.result = 'SUCCESS'
            } catch (exc) {
                // Abort! It does not make sense to continue!
                def error_msg = "Failed to create and setup project environment for ${env.JOB_NAME}!\n"
                error_msg += exc.toString()
                currentBuild.result = 'ABORT'
                error error_msg
            }
        }

        status_msg = ''
        stage('Checkout') {
            try {
                echo "Going to checkout ${env.JOB_NAME}."
                container(name: 'jnlp', shell: '/bin/bash') {
                    checkout scm
                }

                echo status_string(status_msg, 'SUCCESS')
                currentBuild.result = 'SUCCESS'
            } catch (exc) {
                // Abort! It does not make sense to continue!
                def error_msg = "Failed to checkout ${env.JOB_NAME}!\n"
                error_msg exc.toString()
                currentBuild.result = 'ABORT'
                error error_msg
            }
        }

        status_msg = ''
        stage('Unit Test') {
            try {
                echo "Going to run unit tests for ${env.JOB_NAME}."

                // Build shell script to run unit tests.
                //
                // Note that we first need to activate the
                // Python virtualenv that was created in the
                // Setup stage.
                def to_execute = "#!/bin/bash\n" +
                    "source activate jenkinsenv\n" +
                    "tox\n"

                container(name: 'jnlp', shell: '/bin/bash') {
                    status_msg = sh(returnStdout: true, script: to_execute).trim()
                }

                echo status_string(status_msg, 'SUCCESS')
                currentBuild.result = 'SUCCESS'
            } catch (exc) {
                // We do not abort here. We mark this stage as a failure and continue
                // with the cleanup stage.
                def error_msg = "Unit tests failed for ${env.JOB_NAME}!\n"
                error_msg += exc.toString()
                echo error_msg
                currentBuild.result = 'FAILURE'
            }
        }

        status_msg = ''
        stage('Cleanup') {
            try {
                echo "Going to gather test results and reports for ${env.JOB_NAME}."

                container(name: 'jnlp', shell: '/bin/bash') {
                    junit '**/junit-*.xml'
                    try {
                        step([$class: 'CoberturaPublisher', coberturaReportFile: '**/coverage.xml'])
                    } catch (exc) {
                        echo 'Missing Cobertura plugin for coverage reports.'
                    }
                }

                currentBuild.result = 'SUCCESS'
            } catch (exc) {
                echo "Failed to gather test resutls and reports for ${env.JOB_NAME}!"
                currentBuild.result = 'FAILURE'
            } finally {
                // Cleanup workspace
                echo "Cleaning up workspace."
                deleteDir()
            }
        }
    }
}
