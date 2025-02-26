import * as core from '@actions/core';
import {downloadTool, extractTar, cacheDir} from '@actions/tool-cache';
import util from 'node:util';
import child_process from 'node:child_process';
import * as fs from 'fs';
import {promisify} from 'util';

const writeFile = promisify(fs.writeFile);
const exec = util.promisify(child_process.exec);

async function setup() {
  // Fetch user input.
  const hdfsUrl = core.getInput('hdfs-download-url');

  // Download hdfs and extract.
  const hdfsTar = await downloadTool(hdfsUrl);
  const hdfsFolder = (await extractTar(hdfsTar)) + `/hadoop-3.4.1`;

  const coreSite = `<configuration>
    <property>
        <name>fs.defaultFS</name>
        <value>hdfs://localhost:9000</value>
    </property>
    <property>
        <name>hadoop.http.staticuser.user</name>
        <value>runner</value>
    </property>
</configuration>`;
  await writeFile(`${hdfsFolder}/etc/hadoop/core-site.xml`, coreSite);

  const hdfsSite = `<configuration>
    <property>
        <name>dfs.replication</name>
        <value>1</value>
    </property>
    <property>
        <name>dfs.webhdfs.enabled</name>
        <value>true</value>
    </property>
    <property>
        <name>dfs.namenode.http-address</name>
        <value>localhost:9870</value>
    </property>
    <property>
        <name>dfs.secondary.http.address</name>
        <value>localhost:9100</value>
    </property>
</configuration>`;
  await writeFile(`${hdfsFolder}/etc/hadoop/hdfs-site.xml`, hdfsSite);

  const hdfsHome = await cacheDir(hdfsFolder, 'hdfs', '3.4.1');

  // Setup self ssh connection.
  // Fix permission issues: https://github.community/t/ssh-test-using-github-action/166717/12
  const cmd = `set -ex;
  chmod g-w $HOME;
  chmod o-w $HOME;
  ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa;
  cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys;
  chmod 0600 ~/.ssh/authorized_keys;
  ssh-keyscan -H localhost >> ~/.ssh/known_hosts;
  chmod 0600 ~/.ssh/known_hosts;
  eval \`ssh-agent\`;
  ssh-add ~/.ssh/id_rsa;
`;
  let result = await exec(cmd);
  core.info(result.stdout);
  core.warning(result.stderr);

  core.info('Setup self ssh success');

  // Start hdfs daemon.
  result = await exec(`${hdfsHome}/bin/hdfs namenode -format`);
  core.info(result.stdout);
  core.warning(result.stderr);
  core.info('Format hdfs namenode success');

  result = await exec(`${hdfsHome}/sbin/start-dfs.sh`);
  core.info(result.stdout);
  core.warning(result.stderr);
  core.info('Start hdfs success');

  core.addPath(`${hdfsHome}/bin`);
  core.exportVariable('HDFS_NAMENODE_ADDR', '127.0.0.1:9000');
  core.exportVariable('HDFS_NAMENODE_HTTP_ADDR', '127.0.0.1:9870');
  core.exportVariable('HADOOP_HOME', hdfsHome);
}

setup().catch(err => {
  core.error(err);
  core.setFailed(err.message);
});
