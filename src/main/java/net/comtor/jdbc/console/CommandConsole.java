package net.comtor.jdbc.console;

import java.io.BufferedWriter;
import java.io.Console;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.util.Arrays;
import java.util.ResourceBundle;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Implements a simple interactive console
 *
 * @author omarazrat
 */
public class CommandConsole {

    private Connection conn = null;
    private ResourceBundle bundle = null;

    public CommandConsole() {
        bundle = ResourceBundle.getBundle(CommandConsole.class.getCanonicalName());
    }

    /**
     * Beware: don't forget to invoke {@link #setConn(java.sql.Connection) }
     * before calling this method
     */
    public void run() {
        final String[] exitCommands = {"exit", "q"};
        final String[] helpCommands = {"--help", "?"};
        System.out.println(bundle.getString("console.welcome").replace("$v", JDBCSqlConsole.version).replace("$n", JDBCSqlConsole.name));
        Console console = System.console();
        if (console == null) {
            System.err.println(bundle.getString("err.noconsole"));
        }
        String command = console.readLine("> ");
        while (command != null && Arrays.binarySearch(exitCommands, command.toLowerCase()) < 0) {
            String[] commandParts = command.split(" ");
            if (Arrays.binarySearch(helpCommands, command.toLowerCase()) >= 0) {
//System.out.println("simple command caught");
                JDBCSqlConsole.usage();
            } else if (commandParts.length > 1
                    && Arrays.binarySearch(JDBCSqlConsole.commandSupportedCommands, commandParts[0].toLowerCase()) >= 0) {
//System.out.println("meta-command caught");
                String query = commandParts[1];
                JDBCSqlConsole.executeCommand(conn, commandParts[0], query);
            } else {
//System.out.println("db command caught");

                String sql = command;
                while (!sql.trim().endsWith(";")) {
                    sql = sql + console.readLine();
                }
                String outputFName = JDBCSqlConsole.getFilename();
                String cmdOutput = CommandQuery.commandQuery(conn, sql, true, JDBCSqlConsole.getSeparator(), true);
                if (outputFName != null) {
                    try {
                        BufferedWriter writer = new BufferedWriter(new FileWriter(outputFName));
                        writer.append(cmdOutput);
                        writer.close();
                    } catch (IOException ex) {
                        Logger.getLogger(CommandConsole.class.getName()).log(Level.SEVERE, null, ex);
                    }
                } else {
                    System.out.println(cmdOutput);
                }
            }
            System.out.println();
            command = console.readLine("> ");
        }
    }

    public Connection getConn() {
        return conn;
    }

    public void setConn(Connection conn) {
        this.conn = conn;
    }
}
