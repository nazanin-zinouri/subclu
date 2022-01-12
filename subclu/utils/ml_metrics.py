"""
Utilities to standardize getting & logging ML metrics
"""
from collections import defaultdict, Counter
from logging import info
import logging
from pathlib import Path
from typing import Union, Optional, Dict

import mlflow
import numpy as np
import pandas as pd
from sklearn.metrics import precision_recall_fscore_support, confusion_matrix

from .mlflow_logger import rename_for_mlflow


# TODO(djb): create two separate & streamlined functions:
#  one for classification metrics
#  a separate one for confusion matrix (also include confusion matrix plot not just df)
def log_precision_recall_fscore_support(
        y_true: Union[pd.Series, np.array],
        y_pred: Union[pd.Series, np.array],
        data_fold_name: str,
        beta: Union[float, int] = 1,
        average: Optional[str] = 'macro_and_weighted',
        class_labels: iter = None,
        col_class_labels: str = 'class',
        save_path: str = None,
        log_metrics_to_mlflow: bool = True,
        log_artifacts_to_mlflow: bool = False,
        log_metrics_to_console: bool = True,
        log_df_to_console: bool = False,
        log_support: bool = False,
        sort_labels_by_support: bool = False,
        append_fold_name_to_output_dict: bool = True,
        output_dict: bool = False,
) -> Union[pd.DataFrame, Dict[str, float]]:
    """
    Wrapper around `precision_recall_fscore_support` that includes
    - logging metrics to mlflow & console
    - saving artifacts (dataframes) to mlflow

    Args:
        y_true: true values
        y_pred: predicted values
        data_fold_name: name of fold
            e.g., train, test, k=0100 (k-number of clusters)
        beta: for f-score.
            if beta=1, then we get f1_score
        average: whether to average scores
            'None' -> then get results for each class
            'weighted' -> weighted by # of support (observations)
            'macro' -> give same weight to each class
            'avg' -> calculate BOTH 'weighted' and 'macro' and return in a single df
        class_labels:
            provide label names to make classification report easier to read
        col_class_labels:
            name for column with class labels
        save_path:
            string or path to save data locally. Within this path we'll save artifacts with
            a name based on `data_fold_name`
        log_metrics_to_mlflow:
        log_artifacts_to_mlflow:
            if True, log all artifacts in `save_path` to mlflow
        log_metrics_to_console:
            if True, log to metrics to console (log.info)
        log_df_to_console:
            if True, log metrics as dataframe to console (log.info)
        log_support:
            whether to log support "metric" - this might not change if we're using
            the same labels on different k (clusters) so it doesn't make sense to log
            all of them
        sort_labels_by_support:
            If True, then show the most common y_true labels first (at top).
            Only gets applied if class_labels=None.
        output_dict:
            If True, return a dictionary with metrics. Otherwise return a df

    Returns:
        pd.DataFrame or dict with metrics
    """
    if average in ['macro_and_weighted', 'macro', 'weighted']:
        d_class_metrics = defaultdict(list)
        if average == 'macro_and_weighted':
            l_avg_metrics = ['macro', 'weighted']
        else:
            l_avg_metrics = [average]

        for avg_ in l_avg_metrics:
            class_name = f'{avg_}_avg'
            d_class_metrics[col_class_labels].append(class_name)
            score_tuple = precision_recall_fscore_support(y_true, y_pred, beta=beta, average=avg_,
                                                          zero_division=0)
            d_class_metrics['precision'].append(score_tuple[0])
            d_class_metrics['recall'].append(score_tuple[1])
            d_class_metrics[f'f{beta}_score'].append(score_tuple[2])
            d_class_metrics['support'].append(len(y_true))

    else:
        # this branch is for getting metrics for each class individually
        d_class_metrics = dict()
        if sort_labels_by_support & (class_labels is None):
            x = Counter(y_true)
            # update values in case there are y_pred values missing in y_true
            x.update(Counter(set(y_pred)))
            class_labels = [k for k, v in x.most_common()]
        elif class_labels is None:
            class_labels = sorted(set(y_true) | set(y_pred))

        d_class_metrics[col_class_labels] = class_labels
        (
            d_class_metrics['precision'],
            d_class_metrics['recall'],
            d_class_metrics[f'f{beta}_score'],
            d_class_metrics['support']
        ) = precision_recall_fscore_support(y_true, y_pred, beta=beta,
                                            labels=class_labels, zero_division=0,
                                            average=average)

    df = pd.DataFrame(d_class_metrics)

    d_output_metrics = dict()
    for metric_ in [k for k in d_class_metrics.keys() if k != col_class_labels]:
        if all([not log_support, metric_ == 'support']):
            # support should be constant for many cases, so only log it when
            #  requested explicitly
            continue
        for class_, val_ in zip(d_class_metrics[col_class_labels], d_class_metrics[metric_]):
            metric_name = f"{metric_}-{class_}-{data_fold_name}"
            # Note that output dict and (console & mlflow) metrics may have different names!
            #  We sometimes want to keep the metric name constant and store the fold in a different
            #  column
            if append_fold_name_to_output_dict:
                d_output_metrics[metric_name] = val_
            else:
                d_output_metrics[f"{metric_}-{class_}"] = val_

            if log_metrics_to_console:
                info(f"{metric_name}: {val_}")
            if log_metrics_to_mlflow:
                mlflow.log_metric(metric_name, val_)

    # append data fold name to df in case we want to compare multiple dfs
    df['data_fold'] = data_fold_name
    if log_df_to_console:
        info(f"df metrics:\n{df}")

    if save_path is not None:
        Path.mkdir(Path(save_path), parents=True, exist_ok=True)
        df.to_csv(
            Path(save_path) / f"{data_fold_name}-{average}_avg-classification_report.csv",
            index=True
        )

    if log_artifacts_to_mlflow:
        mlflow.log_artifacts(save_path)

    if output_dict:
        return d_output_metrics
    else:
        return df


def log_confusion_matrix(
        y_true: Union[pd.Series, np.array],
        y_pred: Union[pd.Series, np.array],
        data_fold_name: str,
        class_labels: iter = None,
        save_path: str = None,
        log_artifacts_to_mlflow: bool = False,
        log_df_to_console: bool = False,
        sort_labels_by_support: bool = False,
) -> pd.DataFrame:
    """Wrapper around confusion matrix to save as a dataframe for
    downstream analysis.

    class_labels:
        If None is given, labels that appear at least once in y_true
        or y_pred are used in sorted order.

    sort_labels_by_support:
        If True, then show the most common y_true labels first (at top).
        Only gets applied if class_labels=None.
    """
    if sort_labels_by_support & (class_labels is None):
        x = Counter(y_true)
        # update values in case there are y_pred values missing in y_true
        x.update(Counter(set(y_pred)))
        class_labels = [k for k, v in x.most_common()]
    elif class_labels is None:
        class_labels = sorted(set(y_true) | set(y_pred))

    conf_mx = confusion_matrix(y_true, y_pred, labels=class_labels)

    try:
        df_conf_mx = pd.DataFrame(
            conf_mx,
            index=class_labels,
            columns=class_labels
        )
    except Exception as er:
        logging.error(f"Can't save confusion matrix as df\n  {er}")
        df_conf_mx = pd.DataFrame(
            conf_mx,
            index=[rename_for_mlflow(lab) for lab in class_labels],
            columns=[rename_for_mlflow(lab) for lab in class_labels]
        )

    if log_df_to_console:
        info(f"confusion matrix {data_fold_name}:\n{df_conf_mx}")

    if save_path is not None:
        Path.mkdir(Path(save_path), parents=True, exist_ok=True)
        df_conf_mx.to_csv(Path(save_path) / f"{data_fold_name}-confusion_matrix.csv",
                          index=True)

    if log_artifacts_to_mlflow:
        mlflow.log_artifacts(save_path)

    return df_conf_mx


def log_classification_report_and_confusion_matrix(
        y_true: Union[pd.Series, np.array],
        y_pred: Union[pd.Series, np.array],
        data_fold_name: str,
        beta: Union[float, int] = 1,
        class_labels: iter = None,
        sort_labels_by_support: bool = True,
        col_class_labels: str = 'class',
        save_path: str = None,
        log_metrics_to_mlflow: bool = True,
        log_artifacts_to_mlflow: bool = False,
        log_metrics_to_console: bool = True,
        log_df_to_console: bool = False,
        log_support_avg: bool = False,
        log_support_per_class: bool = False,
        output_dict: bool = True,
) -> None:
    """Wrapper around log_precision_recall_fscore_support & log_confusion_matrix
    To run with a single call.

    This one call can trigger both average metrics AND per-class metrics with a single
    call to reduce the number of times we have to copy/paste shared inputs like:
     - path for saving the data
     - class labels
    """
    # log per-class metrics
    log_precision_recall_fscore_support(
        y_true=y_true,
        y_pred=y_pred,
        data_fold_name=data_fold_name,
        beta=beta,
        average=None,
        class_labels=class_labels,
        col_class_labels=col_class_labels,
        save_path=save_path,
        log_metrics_to_mlflow=log_metrics_to_mlflow,
        log_artifacts_to_mlflow=False,
        log_metrics_to_console=log_metrics_to_console,
        log_df_to_console=log_df_to_console,
        log_support=log_support_per_class,
        sort_labels_by_support=sort_labels_by_support,
    )

    # log average data
    log_precision_recall_fscore_support(
        y_true=y_true,
        y_pred=y_pred,
        data_fold_name=data_fold_name,
        beta=beta,
        average='macro_and_weighted',
        class_labels=class_labels,
        col_class_labels=col_class_labels,
        save_path=save_path,
        log_metrics_to_mlflow=log_metrics_to_mlflow,
        log_artifacts_to_mlflow=False,
        log_metrics_to_console=log_metrics_to_console,
        log_df_to_console=log_df_to_console,
        log_support=log_support_avg,
        sort_labels_by_support=sort_labels_by_support,
    )

    df_conf_mx = log_confusion_matrix(
        y_true=y_true,
        y_pred=y_pred,
        data_fold_name=data_fold_name,
        class_labels=class_labels,
        save_path=save_path,
        log_df_to_console=log_df_to_console,
        log_artifacts_to_mlflow=False,
        sort_labels_by_support=sort_labels_by_support,
    )

    # only make a single call to log artifacts mlflow, instead of one call per item
    if log_artifacts_to_mlflow:
        mlflow.log_artifacts(save_path)

    # Add PPV & NPV calculation, this is expected to only work for binary classification
    if len(df_conf_mx) == 2:
        try:
            tn, fp, fn, tp = df_conf_mx.to_numpy().ravel()
            d_extra_metrics = {
                f"{data_fold_name.lower()}-ppv": (tp / (tp + fp)),
                f"{data_fold_name.lower()}-npv": (tn / (tn + fn)),

                f"{data_fold_name.lower()}-tn": tn,
                f"{data_fold_name.lower()}-fp": fp,
                f"{data_fold_name.lower()}-fn": fn,
                f"{data_fold_name.lower()}-tp": tp,
            }

            if log_metrics_to_console:
                for metric, val in d_extra_metrics.items():
                    info(f"{metric}: {val}")

            if log_metrics_to_mlflow:
                mlflow.log_metrics(d_extra_metrics)
        except Exception as er:
            logging.error(f"Can't log binary metrics: {er}")


#
# ~ fin
#
