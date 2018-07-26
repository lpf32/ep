package server

func PrepareJoinCluster(cfg *Config) error {
	// - A PD tries to join itself.
	if cfg.Join == "" {
		return nil
	}

	return nil
}
