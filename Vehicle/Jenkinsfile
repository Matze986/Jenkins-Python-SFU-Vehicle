pipeline {
  agent any
  parameters {
    string(name: 'PackageMetadata', description: 'Metadata of the package as JSON')
    string(name: 'PackageContentS3Key', description: 'S3 key of the package content')
    string(name: 'Email', description: 'User email')
    string(name: 'BaseUrl', description: 'Base service URL')
  }
  stages {
    stage('hello') {
      steps {
        script {
          // Escape JSON input to prevent issues
          def safePackageMetadata = PackageMetadata.replaceAll('"', '\\"')

          // Run Python script and capture status
          def status = sh(script: "python3 hello.py '${safePackageMetadata}' '${PackageContentS3Key}' '${Email}' '${BaseUrl}'", returnStatus: true)

          // Fail pipeline if script fails
          if (status != 0) {
            error("Python script failed!")
          }
        }
      }
    }
  }
}
