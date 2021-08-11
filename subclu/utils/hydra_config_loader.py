"""
Functions & utilities to load hydra config files for data loading &/or modeling
"""
from typing import List

from hydra import initialize, compose
from omegaconf import OmegaConf


class LoadHydraConfig:
    """Hydra-based config to load & override config

    Example uses:
    config_test = AggregateEmbeddingsConfig(
        config_path="../config",
        config_name='aggregate_embeddings',
        overrides=['mlflow_experiment=v0.3.2_use_multi_aggregates_test', 'n_sample_posts=1000', 'n_sample_comments=2000']
    )

    mlflow_experiment_full = 'v0.3.2_use_multi_aggregates'
    config_full_lc_false = AggregateEmbeddingsConfig(
        config_path="../config",
        config_name='aggregate_embeddings',
        overrides=[f"mlflow_experiment={mlflow_experiment_full}",
                   'n_sample_posts=null',
                   'n_sample_comments=null',
                   'data_embeddings_to_aggregate=top_subs-2021_07_16-use_muti_lower_case_false',
                  ]
    )
    """
    def __init__(
            self,
            config_name: str = None,
            config_path: str = "../config",
            overrides: List[str] = None,
    ):
        """

        Args:
            config_path:
                Path to root config, relative to current file
            config_name:
                Name of config, exclude `.yaml` extension
            overrides:
                List of items to override from default config.
                Note: If you add `+` to beginning of override item, it will ADD it, instead of
                overriding it.
        """
        with initialize(config_path=config_path):
            if overrides is not None:
                self.config = compose(config_name=config_name, overrides=overrides)
            else:
                self.config = compose(config_name=config_name)

        self.config_dict = OmegaConf.to_container(self.config)

        # Note: it only flattens one level
        self.config_flat = dict()
        for k, v in self.config_dict.items():
            if isinstance(v, dict):
                for k_nested, v_nested in v.items():
                    self.config_flat[k_nested] = v_nested
            else:
                self.config_flat[k] = v


#
# ~ fin
#
