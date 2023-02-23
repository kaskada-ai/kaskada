package cmd

import (
	"os"
	"strings"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var (
	cfgFile string
)

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "cli",
	Short: "A CLI tool for interacting with the Kaskada API",
	/*
		Long: `A longer description that spans multiple lines and likely contains examples
		and usage of using your command. For example:

		Cobra is a CLI library for Go that empowers applications.
		This application is a tool to generate the needed files
		to quickly create a Cobra application.`,
	*/
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	err := rootCmd.Execute()
	if err != nil {
		os.Exit(1)
	}
}

func init() {
	cobra.OnInitialize(initLogging, initConfig)

	// Here you will define your flags and configuration settings.
	// Cobra supports persistent flags, which, if defined here,
	// will be global for your application.

	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default is $HOME/.cli.yaml)")
	rootCmd.PersistentFlags().String("kaskada-api-server", "", "Kaskada API Server")
	rootCmd.PersistentFlags().String("kaskada-client-id", "", "Kaskada Client ID")
	rootCmd.PersistentFlags().BoolP("debug", "d", false, "get debug log output")
	rootCmd.PersistentFlags().Bool("use-tls", true, "Use TLS when connecting to the Kaskada API")

	viper.BindPFlag("kaskada-api-server", rootCmd.PersistentFlags().Lookup("kaskada-api-server"))
	viper.BindPFlag("kaskada-client-id", rootCmd.PersistentFlags().Lookup("kaskada-client-id"))
	viper.BindPFlag("debug", rootCmd.PersistentFlags().Lookup("debug"))
	viper.BindPFlag("use-tls", rootCmd.PersistentFlags().Lookup("use-tls"))

	viper.SetDefault("kaskada-api-server", "localhost:50051")
	viper.SetDefault("debug", false)
	viper.SetDefault("use-tls", false)

	viper.SetEnvKeyReplacer(strings.NewReplacer("-", "_"))
	viper.AutomaticEnv()
}

func initLogging() {
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})
}

// initConfig reads in config file and ENV variables if set.
func initConfig() {
	if cfgFile != "" {
		// Use config file from the flag.
		viper.SetConfigFile(cfgFile)
	} else {
		// Find home directory.
		home, err := os.UserHomeDir()
		cobra.CheckErr(err)

		// Search config in home directory with name ".cli" (without extension).
		viper.AddConfigPath(home)
		viper.SetConfigType("yaml")
		viper.SetConfigName(".cli")
	}

	viper.AutomaticEnv() // read in environment variables that match

	// If a config file is found, read it in.
	if err := viper.ReadInConfig(); err == nil {
		log.Info().Msgf("Using config file: %s", viper.ConfigFileUsed())
	}

	if viper.GetBool("debug") {
		zerolog.SetGlobalLevel(zerolog.DebugLevel)
	} else {
		zerolog.SetGlobalLevel(zerolog.InfoLevel)
	}
}

func logAndQuitIfErrorExists(err error) {
	if err != nil {
		log.Fatal().Err(err).Send()
	}
}
