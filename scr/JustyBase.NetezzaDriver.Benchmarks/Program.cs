using BenchmarkDotNet.Running;
using JustyBase.NetezzaDriver.Benchmarks;

_ = BenchmarkRunner.Run<Benchy>();
//_ = BenchmarkRunner.Run<ExternalBench>();
