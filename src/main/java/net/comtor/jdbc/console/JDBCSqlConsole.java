package net.comtor.jdbc.console;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.Console;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.*;
import java.util.Arrays;
import java.util.List;
import java.util.ResourceBundle;
import java.util.StringJoiner;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

/**
 *
 * Clase principal
 *
 * @author juriel
 */
public class JDBCSqlConsole {

    private static ResourceBundle bundle = ResourceBundle.getBundle(JDBCSqlConsole.class.getCanonicalName());
    public final static String name = "JDBCSqlConsole";
    public final static String version = "3.0";
    public final static int CMD_QUERY = 1;
    public final static int CMD_EXPORT_CSV = 2;
    public final static int CMD_EXPORT_EXCEL = 3;
    public final static int CMD_GENERATE_CREATE_TABLE = 4;
    public final static int CMD_GENERATE_CREATE_CLASS = 5;
    public static final String DRIVER_MYSQL = "com.mysql.jdbc.Driver";
    public static final String DRIVER_ORACLE = "oracle.jdbc.OracleDriver";
    public static final String DRIVER_POSTGRESQL = "org.postgresql.Driver";
    public static final String DRIVER_SQLSERVER = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
    public static final String DRIVER_SYBASE = "com.sybase.jdbc2.jdbc.SybDriver";
    private static int CMD = -1;
    private static String host = null;
    private static int port = -1;
    private static String database = null;
    private static String driver = null;
    private static String url = null;
    private static String user = null;
    private static String password = null;
    private static String separator = "\t";
    private static String query = null;
    private static String filename = null;
    private static String sourceFilename = null;
    private static boolean showHeaders = true;
    private static boolean showMetaData = false;
    private static boolean showSummary = false;
    private static boolean interactive = false;
    private static boolean displaySourceQuery = false;
    /**
     * Supported commands in the {@link CommandConsole}
     */
    private static final String showmetadataCMD = "-show-metadata";
    private static final String exporttocsvCMD = "-export-to-csv";
    private static final String exporttoexcelCMD = "-export-to-excel";
    private static final String generatecreatetableCMD = "-generate-create-table";
    private static final String generatecreateclassCMD = "-generate-create-class";
    /**
     * Supported commands in the {@link CommandConsole}
     */
    public static final String[] commandSupportedCommands = {exporttocsvCMD, exporttoexcelCMD, generatecreateclassCMD,
        generatecreatetableCMD, showmetadataCMD};

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {

        Options options = new Options();
        options.addOption(Option.builder("?").longOpt("help").desc("Print help").build());
        options.addOption(Option.builder("i").longOpt("interactive").desc("Opens a command shell").build());
        options.addOption(Option.builder().longOpt("driver").hasArg().argName("jdbc driver").desc("JDBC driver i.e: com.mysql.jdbc.Driver").build());

        options.addOption(Option.builder().longOpt("mysql").desc("to use mysql driver try: help.mysql").build());

        options.addOption(Option.builder().longOpt("oracle").desc("to use oracle driver try: help.oracle").build());

        options.addOption(Option.builder().longOpt("postgresql").desc("to use postgresql driver try: help.postgresql").build());

        options.addOption(Option.builder().longOpt("sqlserver").desc("to use sqlserver driver try: help.sqlserver").build());

        options.addOption(Option.builder().longOpt("help.mysql").desc("print mysql help").build());
        options.addOption(Option.builder().longOpt("help.oracle").desc("print oracle help").build());
        options.addOption(Option.builder().longOpt("help.postgresql").desc("print postgresql help").build());
        options.addOption(Option.builder().longOpt("help.sqlserver").desc("print sqlserver help").build());

        options.addOption(Option.builder("u").longOpt("user").hasArg().argName("username/login").desc("user to connnect to database").build());
        options.addOption(Option.builder("p").longOpt("password").hasArg().argName("password").desc("password to connnect to database").build());
        options.addOption(Option.builder("P").desc("with no password: Password will be prompted by the console and won't be visible on screen").build());

        options.addOption(Option.builder().longOpt("url").hasArg().argName("jdbc url").desc("JDBC URL").build());
        options.addOption(Option.builder("h").longOpt("host").hasArg().argName("hostname").desc("hostname").build());
        options.addOption(Option.builder().longOpt("port").hasArg().argName("port").desc("port").build());
        options.addOption(Option.builder().longOpt("database").hasArg().argName("database").desc("database").build());
        options.addOption(Option.builder().longOpt("SID").hasArg().argName("SID").desc("SID for oracle databases").build());

        options.addOption(Option.builder().longOpt("separator").hasArg().argName("column separator").desc("").build());
        options.addOption(Option.builder().longOpt("hide-headers").desc("Hide result headers").build());
        options.addOption(Option.builder().longOpt("show-metadata").desc("show result metadata").build());
        options.addOption(Option.builder().longOpt("out-filename").hasArg().argName("filename").desc("Output filename").build());

        options.addOption(Option.builder().longOpt("export").hasArg().argName("format").desc("formats CSV, XLSX, CREATE_TABLE, JAVA").build());

        CommandLineParser parser = new DefaultParser(true);
        CommandLine commandLine = null;
        try {
            commandLine = parser.parse(options, args);
        } catch (ParseException ex) {
            System.err.println(ex.getMessage());
            HelpFormatter formatter = new HelpFormatter();
            formatter.setOptionComparator(null);
            formatter.printHelp("java -jar jdbc-sql-console.jar", options);
            System.exit(0);
        }

        if (commandLine.hasOption("help")) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.setOptionComparator(null);
            formatter.printHelp("java -jar jdbc-sql-console.jar", options);
            System.exit(0);
        }

        parseArgs(commandLine);
        try {
            Class.forName(driver);

        } catch (Exception e) {
            System.out.println("Unable to load driver " + driver);
            System.out.println("ERROR " + e.getMessage());
            return;
        }

        //List<String> ars = commandLine.getArgList();
        if (commandLine.hasOption("interactive") && (query.length() > 0 )) {
            System.err.println("error interactive with args");
            System.exit(1);
        }
        if (url == null) {
            url = JDBCURLHelper.generateURL(driver, host, port, database);
        }

        try (java.sql.Connection conn = DriverManager.getConnection(url, user, password)) {

            if (commandLine.hasOption("interactive")) {

                final CommandConsole commandConsole = new CommandConsole();
                commandConsole.setConn(conn);
                commandConsole.run();
            } else {
                if (CMD == -1) {
                    CMD = CMD_QUERY;
                }
                executeCommand(conn, CMD, query);
            }
        } catch (SQLException ex) {
            System.out.println("Unable to create connection " + url);
            System.out.println("ERROR " + ex.getMessage());
            return;
        }

    }

    public static void parseArgs(CommandLine cmd) throws NumberFormatException {
        if (cmd.hasOption("url")) {
            url = cmd.getOptionValue("url");
        }
        if (cmd.hasOption("driver")) {
            driver = cmd.getOptionValue("driver");;
        }
        if (cmd.hasOption("mysql")) {
            driver = DRIVER_MYSQL;
        }
        if (cmd.hasOption("postgresql")) {
            driver = DRIVER_POSTGRESQL;
        }
        if (cmd.hasOption("oracle")) {
            driver = DRIVER_ORACLE;
        }
        if (cmd.hasOption("oracle")) {
            driver = DRIVER_SQLSERVER;
        }

        if (cmd.hasOption("host")) {
            host = cmd.getOptionValue("host", "localhost");
        }
        if (cmd.hasOption("port")) {
            port = Integer.parseInt(cmd.getOptionValue("host"));
        }
        if (cmd.hasOption("database")) {
            database = cmd.getOptionValue("database");
        }
        if (cmd.hasOption("sid")) {
            database = cmd.getOptionValue("sid");
        }

        if (cmd.hasOption("user")) {
            user = cmd.getOptionValue("user");
        }
        if (cmd.hasOption("password")) {
            password = cmd.getOptionValue("password");
        }
        if (cmd.hasOption("P")) {
            Console in = System.console();
            System.out.println(bundle.getString("password.prompt"));
            password = new String(in.readPassword());
        }
        if (cmd.hasOption("separator")) {
            separator = cmd.getOptionValue("separator");
        }

        if (cmd.hasOption("hide-headers")) {
            showHeaders = false;
        }
        if (cmd.hasOption("show-metadata")) {
            showMetaData = true;
        }
        if (cmd.hasOption("export")) {
            String format = cmd.getOptionValue("export");
            if (format.equalsIgnoreCase("csv")) {
                CMD = CMD_EXPORT_CSV;
            }
            if (format.equalsIgnoreCase("xlsx")) {
                CMD = CMD_EXPORT_EXCEL;
            }
            if (format.equalsIgnoreCase("class")) {
                CMD = CMD_GENERATE_CREATE_CLASS;
            }
            if (format.equalsIgnoreCase("java")) {
                CMD = CMD_GENERATE_CREATE_CLASS;
            }
            if (format.equalsIgnoreCase("CREATE_TABLE")) {
                CMD = CMD_GENERATE_CREATE_TABLE;
            }
        }
        if (cmd.hasOption("out-filename")) {
            filename = cmd.getOptionValue("out-filename");
        }

        List<String> args = cmd.getArgList();

        StringJoiner j = new StringJoiner(" ");
        for (String arg : args) {
            j.add(arg);
        }
        query = j.toString();
        /*
        else if (str.equals("-separator")) {
        paramCounter++;
        separator = args[paramCounter];
        } else if (str.equals("-hide-headers")) {
        showHeaders = false;
        } else if (str.equals(showmetadataCMD)) {
        showMetaData = true;
        } else if (str.equals("-query")) {
        CMD = CMD_QUERY;
        } else if (str.equals(exporttocsvCMD)) {
        CMD = CMD_EXPORT_CSV;
        } else if (str.equals(exporttoexcelCMD)) {
        CMD = CMD_EXPORT_EXCEL;
        } else if (str.equals(generatecreatetableCMD)) {
        CMD = CMD_GENERATE_CREATE_TABLE;
        } else if (str.equals(generatecreateclassCMD)) {
        CMD = CMD_GENERATE_CREATE_CLASS;
        } else if (str.equals("-filename")) {
        paramCounter++;
        filename = args[paramCounter];
        } else if (str.equals("-source")) {
        paramCounter++;
        sourceFilename = args[paramCounter];
        try {
        getQueryFromFile();
        } catch (IOException ex) {
        ex.printStackTrace();
        }
        } else if (str.equals("-ds")) {
        displaySourceQuery = true;
        } else if (Arrays.binarySearch(interactiveCommands, str.toLowerCase()) >= 0) {
        interactive = true;
        } else if (str.startsWith("--help")) {//General help
        if (size > paramCounter + 1) {
        paramCounter++;
        String ctxhelp = args[paramCounter];
        //Specific help
        if (ctxhelp.toLowerCase().equals("sqlserver")) {
        System.out.println(bundle.getString("sqlserver.help"));
        System.exit(0);
        } else if (ctxhelp.toLowerCase().equals("oracle")) {
        System.out.println(bundle.getString("oracle.help"));
        System.exit(0);
        } else if (ctxhelp.toLowerCase().equals("mysql")) {
        System.out.println(bundle.getString("mysql.help"));
        System.exit(0);
        }
        }
        usage();
        System.exit(0);
        } else {
        query = str;
        }
        }
         */
    }

    protected static void usage() {
        System.out.println(bundle.getString("usage.msg"));
    }

    /**
     * Executes a command given its command name
     */
    protected static void executeCommand(Connection conn, String commandName, String queryStr) {
        int commandId = -1;
        if (commandName.equals(exporttoexcelCMD)) {
            commandId = CMD_EXPORT_EXCEL;
        } else if (commandName.equals(generatecreatetableCMD)) {
            commandId = CMD_GENERATE_CREATE_TABLE;
        } else if (commandName.equals(generatecreateclassCMD)) {
            commandId = CMD_GENERATE_CREATE_CLASS;
        }
        executeCommand(conn, commandId, queryStr);
    }

    /**
     * Executes a command given its id
     */
    private static void executeCommand(Connection conn, int commandId, String queryStr) {
//System.out.println("Command ID:"+commandId+"/query:"+queryStr);
        if (commandId == CMD_QUERY) {
            System.out.println(CommandQuery.commandQuery(conn, queryStr, showHeaders, separator, showMetaData));
        } else if (commandId == CMD_GENERATE_CREATE_TABLE) {
            CommandCreateTable.commandCreateTable(conn, queryStr);
        } else if (commandId == CMD_GENERATE_CREATE_CLASS) {
            CommandCreateClass.commandCreateClass(conn, queryStr);
        } else if (commandId == CMD_EXPORT_EXCEL) {
            CommandQuery.commandQueryExcel(conn, queryStr, showHeaders, showMetaData, filename);
        } else if (commandId == CMD_EXPORT_CSV) {
            if (filename != null) {
                try {
                    BufferedWriter writer = new BufferedWriter(new FileWriter(filename));
                    writer.write(CommandQuery.commandQuery(conn, queryStr, showHeaders, ",", showMetaData));
                    writer.close();

                } catch (IOException ex) {
                    Logger.getLogger(JDBCSqlConsole.class
                            .getName()).log(Level.SEVERE, null, ex);
                }
            }
        }
    }

    /**
     * @return the filename
     */
    public static String getFilename() {
        return filename;
    }

    /**
     * @return the separator
     */
    public static String getSeparator() {
        return separator;
    }

    private static void getQueryFromFile() throws IOException {
        FileReader fr = null;
        BufferedReader br = null;
        try {
            File f = new File(sourceFilename);
            fr = new FileReader(f);
            br = new BufferedReader(fr);

            StringBuilder sb = new StringBuilder();
            String line = null;
            while ((line = br.readLine()) != null) {
                sb.append(line).append("\n");
            }

            query = sb.toString();
            if (displaySourceQuery) {
                System.out.println("----------");
                System.out.println("Query:\n\n" + query);
                System.out.println("----------");
            }
        } finally {
            if (br != null) {
                try {
                    br.close();
                } catch (Exception e) {
                }
            }
            if (fr != null) {
                try {
                    fr.close();
                } catch (Exception e) {
                }
            }
        }
    }
}
