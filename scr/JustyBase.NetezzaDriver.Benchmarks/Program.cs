using BenchmarkDotNet.Running;
using JustyBase.NetezzaDriver.Benchmarks;

_ = BenchmarkRunner.Run<Benchy>();
//_ = BenchmarkRunner.Run<ExternalBench>();

//Benchy benchy = new Benchy();
//benchy.Setup();
//benchy.Query = "/*1*/SELECT *,random(),random(),random() FROM JUST_DATA..FACTPRODUCTINVENTORY FI ORDER BY ROWID LIMIT 500000";
//benchy.JustyNzDriver();