package app

import (
	_ "net/http/pprof"

	"github.com/spf13/cobra"
)

var indexCmd = &cobra.Command{}

// 	Use:   "index redis://[:<password>@]<host>:<port>[/<dbIndex>]",
// 	Short: "Scan keys and save result in a temporary file for further rendering with display command",
// 	Long:  "",
// 	Args:  cobra.MinimumNArgs(1),
// 	Run: func(cmd *cobra.Command, args []string) {
// 		go func() {
// 			http.ListenAndServe("0.0.0.0:8080", nil)
// 		}()

// 		consoleLogger := logger.NewConsoleLogger(logLevel)
// 		consoleLogger.Info().Msg("Start indexing")

// 		var redisService scanner.RedisServiceInterface
// 		if redisVersion == 0 {
// 			redisAddr := "11.168.176.16:6379"
// 			c := redis.NewClient(&redis.Options{
// 				Addr:     redisAddr,
// 				Password: "Tencent88", // no password set
// 			})
// 			// option, err := redis.ParseURL(args[0])
// 			// if err != nil {
// 			// 	consoleLogger.Fatal().Err(err).Msg("Can't create redis client")
// 			// }
// 			// c = redis.NewClient(option)
// 			redisService = adapter.NewTencentCloudRedisService(c)
// 		} else {
// 			// clientSource, err := (radix.PoolConfig{}).New(context.Background(), "tcp", args[0])
// 			// if err != nil {
// 			// 	consoleLogger.Fatal().Err(err).Msg("Can't create redis client")
// 			// }
// 			// redisService = adapter.NewRedisService(clientSource)
// 		}

// 		redisScanner := scanner.NewScanner(
// 			redisService,
// 			adapter.NewPrettyProgressWriter(os.Stdout),
// 			consoleLogger,
// 		)

// 		resultTrie := typetrie.NewTypeTrie(splitter.NewSimpleSplitter(separators))
// 		redisScanner.Scan(
// 			adapter.ScanOptions{
// 				ScanCount: scanCount,
// 				Pattern:   pattern,
// 				Throttle:  throttleNs,
// 			},
// 			resultTrie,
// 		)
// 		resultTrie.Clean()

// 		indexFileName := os.TempDir() + "/redis-inventory.json"
// 		f, err := os.Create(indexFileName)
// 		if err != nil {
// 			consoleLogger.Fatal().Err(err).Msg("Can't create renderer")
// 		}

// 		r := renderer.NewJSONRenderer(f, renderer.JSONRendererParams{})

// 		err = r.Render(resultTrie.Root())
// 		if err != nil {
// 			consoleLogger.Fatal().Err(err).Msg("Can't write to file")
// 		}

// 		consoleLogger.Info().Msgf("Finish scanning and saved index as a file %s", indexFileName)
// 	},
// }

func init() {
	RootCmd.AddCommand(indexCmd)
	indexCmd.Flags().StringVarP(&logLevel, "logLevel", "l", "info", "Level of logs to be displayed")
	indexCmd.Flags().StringVarP(&separator, "separator", "s", "_", "Symbol that split key pattern")
	indexCmd.Flags().IntVarP(&minPatternNumber, "minPatternNumber", "m", 10,
		"Minium number of keys one pattern must have. Otherwise it's classified as others kind")
	indexCmd.Flags().IntVarP(&scanCount, "scanCount", "c", 1000,
		"Number of keys to be scanned in one iteration (argument of scan command)")
	indexCmd.Flags().IntVarP(&throttleNs, "throttle", "t", 0, "Throttle: number of nanoseconds to sleep between keys")
}
