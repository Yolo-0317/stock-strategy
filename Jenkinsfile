pipeline {
    agent {
        label 'prod-anhui-01'
    }
    
    environment {
        SUPPORTED_PROVINCES = "Anhui,Jiangsu,Shandong"
    }

    parameters {
        string(name: 'PROJECT_NAME', defaultValue: '电力现货市场数据爬取前端', description: 'Jenkins 项目名称')
        string(name: 'GIT_URL', defaultValue: 'git@codeup.aliyun.com:684f77a9d27b6b3c0e19b96a/luminai-model/luminai-crawler-frontend.git', description: '云效代码仓库地址')
        string(name: 'INPUT_PROVINCES', defaultValue: 'Anhui,Jiangsu,Shandong', description: '需要部署的省份，用逗号分隔')
    }
    
    stages {
        stage('Echo') {
            steps {
                script {
                    echo "project_name: ${params.PROJECT_NAME}"
                    echo "git_url: ${params.GIT_URL}"
                    echo "input_provinces: ${params.INPUT_PROVINCES}"
                }
            }
        }
        stage('检查省份合法性') {
            steps {
                script {
                    def supported = SUPPORTED_PROVINCES.split(',')
                    def input = params.INPUT_PROVINCES.tokenize(',').collect { it.trim() }
                    def invalid = input.findAll { !supported.contains(it) }

                    if (invalid) {
                        error("以下省份暂不支持部署: ${invalid.join(', ')}")
                    }
                }
            }
        }
        stage('初始化 SSH') {
            steps {
                script {
                    sh '''
                        mkdir -p ~/.ssh
                        ssh-keyscan codeup.aliyun.com >> ~/.ssh/known_hosts
                    '''
                }
            }
        }
        stage('同步代码仓库') {
            steps {
                script {
                    checkout scmGit(
                        branches: [[name: '*/master']], 
                        extensions: [
                            firstBuildChangelog(makeChangelog: true),
                            [$class: 'WipeWorkspace'],  // 清空 workspace，确保干净
                            [$class: 'CleanBeforeCheckout'],  // 清理 .git 工作目录状态
                            [$class: 'LocalBranch', localBranch: 'master']  // 将远程 master 检出为本地 master，非 detached HEAD
                        ], 
                        userRemoteConfigs: [
                            [credentialsId: 'trade-automation-robot-ssh-private-key', url: "${params.GIT_URL}"]
                        ]
                    )
                    
                    env.commitId = sh(script: "git rev-parse HEAD", returnStdout: true).trim()
                    env.commitAuthor = sh(script: "git log -1 --pretty=format:'%an'", returnStdout: true).trim()
                    env.commitMessage = sh(script: "git log -1 --pretty=%B", returnStdout: true).trim()
                    def timestamp = sh(script: "git log -1 --pretty=format:'%ct'", returnStdout: true).trim()
                    env.commitTime = new Date(Long.parseLong(timestamp) * 1000)
                        
                    // if (currentBuild.changeSets.size() > 0) {
                    //     def lastEntry = currentBuild.changeSets.collectMany { it.items }.last()
                    //     env.commitId = lastEntry.commitId
                    //     env.commitAuthor = lastEntry.author
                    //     env.commitMessage = lastEntry.msg
                    //     env.commitTime = new Date(lastEntry.timestamp)
                    // }

                    // 设置镜像 tag
                    def shortCommit = env.commitId.substring(0, 8)
                    def date = sh(script: 'date +%Y%m%d', returnStdout: true).trim()
                    env.IMAGE_TAG = "${date}.${shortCommit}"

                    // echo
                    echo "Image Tag: ${env.IMAGE_TAG}"
                    echo "Last Commit ID: ${env.commitId}"
                    echo "Last Commit Author: ${env.commitAuthor}"
                    echo "Last Commit Message: ${env.commitMessage}"
                    echo "Last Commit Time: ${env.commitTime}"

                    // 中间产物输出
                    def currentTime = new Date().format('yyyy-MM-dd_HH-mm-ss')
                    def record = [
                        time: currentTime,
                        commitId: env.commitId,
                        commitAuthor: env.commitAuthor,
                        commitMessage: env.commitMessage,
                        commitTime: env.commitTime
                    ]
                    def json = new groovy.json.JsonBuilder(record).toPrettyString()
                    writeFile file: 'commit_record.json', text: json, encoding: 'UTF-8'
                    sh "cat commit_record.json"
                }
            }
        }
        stage('Tagging') {
            steps {
                script {
                    def tagName = "v${env.IMAGE_TAG}"
                    
                    sh "git config user.name 'Jenkins'"
                    sh "git config user.email 'jenkins@ci'"
                    
                    // 删除本地已有 tag
                    sh "git tag -d ${tagName} || true"
                    // 添加新 tag
                    sh "git tag -a ${tagName} -m 'Jenkins 自动化部署 ${env.IMAGE_TAG}'"

                    // 推送 tag
                    withCredentials([sshUserPrivateKey(credentialsId: 'trade-automation-robot-ssh-private-key', keyFileVariable: 'SSH_KEY')]) {
                        sh """
                            # 设置 SSH 使用指定密钥
                            export GIT_SSH_COMMAND="ssh -i \$SSH_KEY -o IdentitiesOnly=yes -o StrictHostKeyChecking=no"
                            
                            # 删除远端已有 tag
                            git push origin :refs/tags/${tagName} || true
                            
                            # 推送 tag
                            git push origin ${tagName}
                        """
                    }
                }
            }
            post {
                always {
                    sh "git tag -d v${env.IMAGE_TAG} || true"
                }
            }
        }
        stage('部署总任务') {
            steps {
                script {
                    def dockers = [:]
                    def supported_provinces = env.SUPPORTED_PROVINCES.tokenize(',')
                    def input_provinces = params.INPUT_PROVINCES.tokenize(',')
                    supported_provinces.each { province ->
                        stage("部署分任务 ${province}") {
                            if (!input_provinces.contains(province)) {
                                echo "跳过本次不部署的省份: ${province}"
                                return
                            }
                    
                            dir("${province}/frontend_monkey") {
                                def province_lowercase = province.toLowerCase()
                                sh "TAG=${env.IMAGE_TAG} ./crawler_frontend_build.sh"
                                dockers["${province}_image"] = "${province_lowercase}_crawler_frontend_prod:${env.IMAGE_TAG}"
                                sh "TAG=${env.IMAGE_TAG} ./crawler_frontend_run.sh"
                                dockers["${province}_container"] = "${province_lowercase}_crawler_frontend_prod_instance"
                            }
                        }
                    }
                    def json = new groovy.json.JsonBuilder(dockers).toPrettyString()
                    writeFile file: 'build_record.json', text: json, encoding: 'UTF-8'
                    sh "cat build_record.json"
                }
            }
        }
        stage('Artifacts') {
            steps {
                archiveArtifacts artifacts: 'commit_record.json', fingerprint: true, onlyIfSuccessful: true
                archiveArtifacts artifacts: 'build_record.json', fingerprint: true, onlyIfSuccessful: true
            }
        }
    }
}
