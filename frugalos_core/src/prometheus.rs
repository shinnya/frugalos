//! Prometheus utilities.

use prometrics::metrics::HistogramBuilder;

/// ヒストグラムの区間・階級。
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct HistogramBucket(Vec<f64>);
impl HistogramBucket {
    /// `HistogramBucket` を生成して返す。
    pub fn new(bucket: Vec<f64>) -> HistogramBucket {
        Self(bucket)
    }
}

/// メトリクスに適用する設定値。
// コード上でメトリクス名とメトリクス型が紐付いているため、
// メトリクスに対応する型を設定では意識する必要がなく共通の構造を使う。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MetricsOption {
    /// 設定対象のメトリクス名(`namespace`, `subsystem` を除く)。
    ///
    /// 例: rejected_proposal_duration_seconds
    name: String,

    /// メトリクスに設定するバケット値。
    #[serde(default)]
    bucket: HistogramBucket,
}

/// Prometheus のメトリクス設定群を表現する。
///
/// # Example
///
/// 設定ファイルには以下の形式で記載する:
///
/// ```yaml
/// ---
/// metrics:
///   - name: rejected_proposal_duration_seconds
///     bucket:
///       - 0.5
///       - 1.0
///       - 5.0
/// ```
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct PrometheusConfig {
    /// メトリクスの設定値群。
    #[serde(default)]
    metrics: Vec<MetricsOption>,
}
impl PrometheusConfig {
    /// ヒストグラム用の設定を適用する。
    pub fn configure_histogram<F>(&self, name: &str, f: F) -> HistogramBuilder
    where
        F: Fn(&mut HistogramBuilder) -> &mut HistogramBuilder,
    {
        let mut builder = HistogramBuilder::new(name);
        let _ = f(&mut builder);
        if let Some(opts) = self.metrics.iter().find(|opts| opts.name == name) {
            for n in opts.bucket.0.iter() {
                builder.bucket(*n);
            }
        }
        builder
    }

    /// `PrometheusConfig` を生成して返す。
    pub fn new() -> Self {
        Self {
            metrics: Vec::new(),
        }
    }

    /// ヒストグラム用のメトリクス設定を定義する。
    pub fn declare_histogram(mut self, name: &str, bucket: Vec<f64>) -> Self {
        self.metrics.push(MetricsOption {
            name: name.to_owned(),
            bucket: HistogramBucket::new(bucket),
        });
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        let config = PrometheusConfig::new()
            .declare_histogram("rejected_proposal_duration_seconds", vec![0.5, 1.0, 5.0])
            .declare_histogram("request_duration_seconds", vec![0.5, 1.0]);
        //println!("{}", serde_yaml::to_string(&config).unwrap());
        let expected = r##"---
metrics:
  - name: rejected_proposal_duration_seconds
    bucket:
      - 0.5
      - 1.0
      - 5.0
  - name: request_duration_seconds
    bucket:
      - 0.5
      - 1.0"##;
        assert_eq!(expected, serde_yaml::to_string(&config).unwrap());
    }
    #[test]
    fn configure_histogram_works() {
        let metric_name = "request_duration_seconds";
        let config = PrometheusConfig::new().declare_histogram(metric_name, vec![0.5, 1.0]);
        let histogram = config
            .configure_histogram(metric_name, |builder: &mut _| builder.namespace("frugalos"))
            .finish()
            .unwrap();
        assert_eq!(metric_name, histogram.metric_name().name());
    }
}
