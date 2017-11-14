package com.github.animeshtrivedi.FileBench;

import org.apache.commons.cli.*;

/**
 * Created by atr on 9/30/16.
 */
public class ParseOptions {
    private Options options;
    private String test;
    private int parallel;
    private String inputDir;

    public ParseOptions(){
        options = new Options();
        this.test = "sffread";
        this.parallel = 16;
        this.inputDir = "/sql/tpcds-sff/store_sales/";
        options.addOption("h", "help", false, "show help.");
        options.addOption("i", "input", true, "input directory location on HDFS-fs.");
        options.addOption("t", "test", true, "test.");
        options.addOption("p", "parallel", true, "parallel instances.");
    }

    public int getParallel(){
        return this.parallel;
    }

    public String getInputDir(){
        return this.inputDir;
    }

    public void show_help() {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("Main", options);
    }
    public void parse(String[] args) {
        CommandLineParser parser = new GnuParser();
        CommandLine cmd = null;
        try {
            cmd = parser.parse(options, args);

            if (cmd.hasOption("h")) {
                show_help();
                System.exit(0);
            }
        } catch (ParseException e) {
            System.err.println("Failed to parse command line properties" + e);
            show_help();
            System.exit(-1);
        }
    }
}
