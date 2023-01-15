/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.lealone.plugins.createapp;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.Locale;

import freemarker.cache.ClassTemplateLoader;
import freemarker.template.Configuration;
import freemarker.template.Template;

public class CreateLealoneApp {

    public static void main(String[] args) throws Exception {
        CreateLealoneApp app = new CreateLealoneApp();
        app.parseArgs(args);
        app.init();
        app.run();
    }

    private boolean createDal = true;
    private boolean createWeb = true;
    private boolean createTest = true;
    private boolean singleModule = true;

    private String appBaseDir = ".";
    private String appName;
    private String appClassName;

    private String pomName;
    private String groupId;
    private String artifactId;
    private String version = "1.0.0";
    private String packageName;
    private String dbName;

    private String encoding = "UTF-8";

    private File parentDir;

    public String getAppBaseDir() {
        return appBaseDir;
    }

    public String getAppName() {
        return appName;
    }

    public String getAppClassName() {
        return appClassName;
    }

    public String getPomName() {
        return pomName;
    }

    public String getGroupId() {
        return groupId;
    }

    public String getArtifactId() {
        return artifactId;
    }

    public String getVersion() {
        return version;
    }

    public String getPackageName() {
        return packageName;
    }

    public String getDbName() {
        return dbName;
    }

    private void parseArgs(String[] args) {
        for (int i = 0; i < args.length; i++) {
            if (args[i] == null) {
                error("选项参数不能包含null");
            }
            args[i] = args[i].trim();
        }

        try {
            for (int i = 0; i < args.length; i++) {
                String a = args[i];
                if (a.equals("-help")) {
                    printUsageAndExit();
                }

                switch (a) {
                case "-createDal":
                    createDal = Boolean.parseBoolean(args[++i]);
                    break;
                case "-createWeb":
                    createWeb = Boolean.parseBoolean(args[++i]);
                    break;
                case "-createTest":
                    createTest = Boolean.parseBoolean(args[++i]);
                    break;
                case "-singleModule":
                    singleModule = Boolean.parseBoolean(args[++i]);
                    break;
                case "-appBaseDir":
                    appBaseDir = args[++i];
                    break;
                case "-appName":
                    appName = args[++i];
                    break;
                case "-appClassName":
                    appClassName = args[++i];
                    break;
                case "-dbName":
                    dbName = args[++i];
                    break;
                case "-groupId":
                    groupId = args[++i];
                    break;
                case "-artifactId":
                    artifactId = args[++i];
                    break;
                case "-version":
                    version = args[++i];
                    break;
                case "-packageName":
                    packageName = args[++i];
                    break;
                case "-encoding":
                    encoding = args[++i];
                    break;
                default:
                    error("选项名 '" + a + "' 无效");
                    printUsageAndExit();
                }
            }

        } catch (Exception e) {
            printErrorAndExit("参数解析错误", e);
        }
    }

    private void init() {
        if (appName == null && artifactId == null)
            error("必须指定 appName 和 artifactId 其中之一");

        if (groupId == null)
            error("必须指定 groupId");

        if (artifactId == null)
            artifactId = appName;

        if (appName == null)
            appName = artifactId;

        if (packageName == null)
            packageName = groupId;

        if (pomName == null)
            pomName = appName + " project";

        if (dbName == null)
            dbName = artifactId.replace('-', '_');

        if (appClassName == null)
            appClassName = toClassName(appName);
        parentDir = new File(appBaseDir, appName);

        initFreeMarker();
    }

    private static void printUsage() {
        System.out.println();
        System.out.println("用法: java -jar lealone-create-app-1.0.0.jar [选项]");
        System.out.println();
        System.out.println("支持以下选项：");

        println("-help", "打印帮助信息");

        println("-appBaseDir <目录>", "应用根目录 (默认是当前目录)");
        println("-appName <名称>", //
                "应用名称 (如果不指定，默认取 -artifactId 的值，-appName 和 -artifactId 必须至少设置一个)");
        println("-groupId <id>", "pom.xml 的 groupId (必须设置)");
        println("-artifactId <id>", //
                "pom.xml 的 artifactId  (如果不指定，默认取 -appName 的值，-appName 和 -artifactId 必须至少设置一个)");
        println("-version <版本号>", "pom.xml 的项目初始版本号 (默认是1.0.0)");

        println("-packageName <包名>", "项目代码的包名 (如果不指定，默认取 -groupId 的值)");
        println("-dbName <名称>", "数据库名称 (如果不指定，默认取 -artifactId 的值)");

        println("-encoding <编码>", "指定生成的文件采用的编码 (默认是 UTF-8)");

        println("-singleModule <true|false>", "是否是单模块项目 (默认是 true)");
    }

    private static void println(String s1, String s2) {
        System.out.println("\t" + s1);
        System.out.println("\t\t" + s2);
        System.out.println();
    }

    private static void printErrorAndExit(String format, Object... args) {
        System.out.println(String.format(format, args));
        printUsage();
        System.exit(-1);
    }

    private static void printUsageAndExit() {
        printUsage();
        System.exit(-1);
    }

    private static void error(String msg) {
        System.out.println(msg);
        printUsageAndExit();
    }

    private void deleteRecursive(File file) throws IOException {
        if (file.isDirectory()) {
            for (File f : file.listFiles()) {
                deleteRecursive(f);
            }
        }
        file.delete();
    }

    private void run() throws Exception {
        if (parentDir.exists())
            deleteRecursive(parentDir);
        parentDir.mkdirs();

        if (singleModule) {
            writeSingleModule();
        } else {
            writeMultiModule();
        }

        System.out.println(appName + " app created, dir: " + parentDir.getCanonicalPath());
    }

    private void writeSingleModule() throws Exception {
        writeSingleModuleTopFiles();
        if (createDal)
            writeSingleModuleDalFiles();
        writeSingleModuleDistFiles();
        writeSingleModuleMainFiles();
        writeSingleModuleServiceFiles();
        if (createTest)
            writeSingleModuleTestFiles();
        if (createWeb)
            writeSingleModuleWebFiles();
    }

    private void writeMultiModule() throws Exception {
        writeTopFiles();
        if (createDal)
            writeDalFiles();
        writeDistFiles();
        writeMainFiles();
        writeServiceFiles();
        if (createTest)
            writeTestFiles();
        if (createWeb)
            writeWebFiles();
    }

    private final String srcMainJava = "src/main/java";
    private final String srcMainResources = "src/main/resources";
    private final String srcTestJava = "src/test/java";
    private final String srcTestResources = "src/test/resources";

    private void createSrcMainJava(File parentDir) throws Exception {
        new File(parentDir, srcMainJava).mkdirs();
    }

    private void createSrcMainResources(File parentDir) throws Exception {
        new File(parentDir, srcMainResources).mkdirs();
    }

    private void createSrcTestJava(File parentDir) throws Exception {
        new File(parentDir, srcTestJava).mkdirs();
    }

    private void createSrcTestResources(File parentDir) throws Exception {
        new File(parentDir, srcTestResources).mkdirs();
    }

    private File createModuleDir(String moduleName) throws Exception {
        File moduleDir = getModuleDir(moduleName);
        moduleDir.mkdir();
        return moduleDir;
    }

    private File getModuleDir(String moduleName) {
        return new File(parentDir, appName + "-" + moduleName);
    }

    private void writeTopFiles() throws Exception {
        writeFile("/mm/top/build.bat", parentDir);
        writeFile("/mm/top/build.sh", parentDir);
        writeFile("/mm/top/pom.xml", parentDir);
        writeFile("/mm/top/README.md", parentDir);
    }

    private void writeSingleModuleTopFiles() throws Exception {
        writeFile("/sm/top/build.bat", parentDir);
        writeFile("/sm/top/build.sh", parentDir);
        writeFile("/sm/top/pom.xml", parentDir);
        writeFile("/sm/top/README.md", parentDir);
    }

    private void writeDalFiles() throws Exception {
        File moduleDir = createModuleDir("dal");
        createSrcMainJava(moduleDir);
        createSrcMainResources(moduleDir);
        new File(moduleDir, srcMainJava + "/" + packageName.replace('.', '/') + "/dal").mkdirs();
        File toDir = new File(moduleDir, srcMainResources);
        writeFile("/mm/dal/init-data.sql", toDir);
        writeFile("/mm/dal/tables.sql", toDir);
        writeFile("/mm/dal/pom.xml", moduleDir);
    }

    private void writeSingleModuleDalFiles() throws Exception {
        new File(parentDir, srcMainJava + "/" + packageName.replace('.', '/') + "/model").mkdirs();
        File toDir = new File(parentDir, "sql");
        toDir.mkdir();
        writeFile("/sm/sql/init-data.sql", toDir);
        writeFile("/sm/sql/tables.sql", toDir);
    }

    private void writeDistFiles() throws Exception {
        File moduleDir = createModuleDir("dist");

        File binDir = new File(moduleDir, "bin");
        binDir.mkdir();
        writeFile("/mm/dist/bin/lealone.bat", binDir);
        writeFile("/mm/dist/bin/lealone.sh", binDir);
        writeFile("/mm/dist/bin/runSqlScript.bat", binDir);
        writeFile("/mm/dist/bin/runSqlScript.sh", binDir);
        writeFile("/mm/dist/bin/sqlshell.bat", binDir);
        writeFile("/mm/dist/bin/sqlshell.sh", binDir);

        File confDir = new File(moduleDir, "conf");
        confDir.mkdir();
        writeFile("/mm/dist/conf/lealone.yaml", confDir);
        writeFile("/mm/dist/conf/log4j2.xml", confDir);

        writeFile("/mm/dist/assembly.xml", moduleDir);
        writeFile("/mm/dist/pom.xml", moduleDir);
    }

    private void writeSingleModuleDistFiles() throws Exception {
        File moduleDir = new File(parentDir, "dist");
        moduleDir.mkdir();
        File binDir = new File(moduleDir, "bin");
        binDir.mkdir();
        writeFile("/sm/dist/bin/lealone.bat", binDir);
        writeFile("/sm/dist/bin/lealone.sh", binDir);
        writeFile("/sm/dist/bin/runSqlScript.bat", binDir);
        writeFile("/sm/dist/bin/runSqlScript.sh", binDir);
        writeFile("/sm/dist/bin/sqlshell.bat", binDir);
        writeFile("/sm/dist/bin/sqlshell.sh", binDir);

        File confDir = new File(moduleDir, "conf");
        confDir.mkdir();
        writeFile("/sm/dist/conf/lealone.yaml", confDir);
        writeFile("/sm/dist/conf/log4j2.xml", confDir);

        writeFile("/sm/dist/assembly.xml", moduleDir);
    }

    private void writeMainFiles() throws Exception {
        File moduleDir = createModuleDir("main");
        createSrcMainJava(moduleDir);
        File toDir = new File(moduleDir, srcMainJava);
        toDir = new File(toDir, packageName.replace('.', '/') + "/main");
        toDir.mkdirs();
        writeFile("/mm/main/App.java.ftl", toDir, appClassName + ".java");
        writeFile("/mm/main/SqlScript.java.ftl", toDir, appClassName + "SqlScript.java");
        writeFile("/mm/main/pom.xml", moduleDir);
    }

    private void writeSingleModuleMainFiles() throws Exception {
        File toDir = new File(parentDir, srcMainJava);
        toDir = new File(toDir, packageName.replace('.', '/') + "/main");
        toDir.mkdirs();
        writeFile("/sm/main/App.java.ftl", toDir, appClassName + ".java");
        writeFile("/sm/main/SqlScript.java.ftl", toDir, appClassName + "SqlScript.java");
    }

    private void writeServiceFiles() throws Exception {
        File moduleDir = createModuleDir("service");
        createSrcMainJava(moduleDir);
        createSrcMainResources(moduleDir);
        new File(moduleDir, srcMainJava + "/" + packageName.replace('.', '/') + "/service").mkdirs();
        File toDir = new File(moduleDir, srcMainResources);
        writeFile("/mm/service/services.sql", toDir);
        writeFile("/mm/service/pom.xml", moduleDir);
    }

    private void writeSingleModuleServiceFiles() throws Exception {
        new File(parentDir, srcMainJava + "/" + packageName.replace('.', '/') + "/service").mkdirs();
        File toDir = new File(parentDir, "sql");
        writeFile("/sm/sql/services.sql", toDir);
    }

    private void writeTestFiles() throws Exception {
        File moduleDir = createModuleDir("test");
        createSrcTestJava(moduleDir);
        createSrcTestResources(moduleDir);
        new File(moduleDir, srcTestJava + "/" + packageName.replace('.', '/') + "/test").mkdirs();
        File toDir = new File(moduleDir, srcTestResources);
        writeFile("/mm/test/lealone.yaml", toDir);
        writeFile("/mm/test/log4j2-test.xml", toDir);
        writeFile("/mm/test/pom.xml", moduleDir);

        toDir = new File(moduleDir, srcTestJava);
        toDir = new File(toDir, packageName.replace('.', '/') + "/test");
        toDir.mkdirs();
        writeFile("/mm/test/AppTest.java.ftl", toDir, appClassName + "Test.java");
        writeFile("/mm/test/SqlScriptTest.java.ftl", toDir, appClassName + "SqlScriptTest.java");
        writeFile("/mm/test/TemplateCompilerTest.java.ftl", toDir,
                appClassName + "TemplateCompilerTest.java");
    }

    private void writeSingleModuleTestFiles() throws Exception {
        new File(parentDir, srcTestJava + "/" + packageName.replace('.', '/') + "/test").mkdirs();
        File toDir = new File(parentDir, srcTestResources);
        toDir.mkdirs();
        writeFile("/sm/test/lealone.yaml", toDir);
        writeFile("/sm/test/log4j2-test.xml", toDir);

        toDir = new File(parentDir, srcTestJava);
        toDir = new File(toDir, packageName.replace('.', '/') + "/test");
        toDir.mkdirs();
        writeFile("/sm/test/AppTest.java.ftl", toDir, appClassName + "Test.java");
        writeFile("/sm/test/SqlScriptTest.java.ftl", toDir, appClassName + "SqlScriptTest.java");
        // writeFile("/sm/test/TemplateCompilerTest.java.ftl", toDir,
        // appClassName + "TemplateCompilerTest.java");
    }

    private void writeWebFiles() throws Exception {
        File moduleDir = createModuleDir("web");
        createSrcMainJava(moduleDir);
        File toDir = new File(moduleDir, srcMainJava);
        toDir = new File(toDir, packageName.replace('.', '/'));
        toDir.mkdirs();

        File webDir = new File(toDir, "web");
        webDir.mkdir();
        writeFile("/mm/web/Router.java.ftl", webDir, appClassName + "Router.java");

        writeFile("/mm/web/pom.xml", moduleDir);

        webDir = new File(moduleDir, "web");
        webDir.mkdir();
        writeFile("/mm/web/index.html", webDir);
    }

    private void writeSingleModuleWebFiles() throws Exception {
        File toDir = new File(parentDir, srcMainJava + "/" + packageName.replace('.', '/') + "/web");
        toDir.mkdirs();
        writeFile("/sm/web/Router.java.ftl", toDir, appClassName + "Router.java");

        File webDir = new File(parentDir, "web");
        webDir.mkdir();

        writeFile("/sm/web/index.html", webDir);
        copyFile("/sm/web/axios.min-0.21.1.js", webDir);
        copyFile("/sm/web/vue.min-2.3.3.js", webDir);
        copyFile("/sm/web/lealone-rpc-5.0.0.js", webDir);
    }

    private Configuration freeMarkerConfiguration;

    private void initFreeMarker() {
        freeMarkerConfiguration = new Configuration();
        freeMarkerConfiguration.setDefaultEncoding(encoding);
        freeMarkerConfiguration.setLocale(Locale.ENGLISH);
        freeMarkerConfiguration.setTemplateLoader(new ClassTemplateLoader(this.getClass(), "/"));
    }

    private void copyFile(String srcFile, File toDir) throws Exception {
        String s = srcFile;
        srcFile = srcMainResources + srcFile;
        String fileName = new File(srcFile).getName();
        try (InputStream in = new File(srcFile).exists() ? new FileInputStream(srcFile)
                : getClass().getResourceAsStream(s);
                OutputStream out = new FileOutputStream(new File(toDir, fileName))) {
            int n = 0;
            byte[] buffer = new byte[4096];
            while (-1 != (n = in.read(buffer))) {
                out.write(buffer, 0, n);
            }
        }
    }

    private void writeFile(String srcFile, File toDir) throws Exception {
        String fileName = new File(srcFile).getName();
        writeFile(srcFile, toDir, fileName);
    }

    private void writeFile(String srcFile, File toDir, String fileName) throws Exception {
        Template template = freeMarkerConfiguration.getTemplate(srcFile);
        try (BufferedWriter out = new BufferedWriter(
                new OutputStreamWriter(new FileOutputStream(new File(toDir, fileName)), encoding))) {
            template.process(this, out);
        }
    }

    public static String toClassName(String str) {
        str = toCamelFrom(str, "-");
        if (str.indexOf('_') >= 0) {
            str = toCamelFrom(str, "_");
        }
        return str;
    }

    private static String toCamelFrom(String str, String regex) {
        String[] vals = str.split(regex);
        StringBuilder result = new StringBuilder();
        for (int i = 0; i < vals.length; i++) {
            char c = Character.toUpperCase(vals[i].charAt(0));
            result.append(c);
            result.append(vals[i].substring(1));
        }
        return result.toString();
    }
}
