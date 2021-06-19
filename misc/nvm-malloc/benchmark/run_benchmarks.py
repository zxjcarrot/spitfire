import argparse
import os
import subprocess

import matplotlib as mpl
mpl.use("Agg")
from pylab import *

# make plots look better
plt.rcParams['text.latex.preamble']=[r"\usepackage{lmodern}"]
params = {'text.usetex' : True,
          'font.size' : 11,
          'font.family' : 'lmodern',
          'text.latex.unicode': True,
          }
plt.rcParams.update(params)

JEMALLOC_PATH = "/usr/lib/libjemalloc.so"
BENCHMARKS = ["alloc_free", "alloc_free_alloc", "fastalloc", "linkedlist"]
BENCHTITLES = {"alloc_free": "Allocate and Free",
               "alloc_free_alloc": "Allocate, Free and Allocate",
               "fastalloc": "Allocation Loop",
               "linkedlist": "Linked List Creation",
               "recovery": "nvm\_malloc Internal Recovery"}

def getCacheFileName(binary, args, with_jemalloc):
    return os.path.join(os.getcwd(), "cached", "%s_%d_%d_%d_%d_%s" % (binary,
                                                                      args["threads_min"],
                                                                      args["threads_max"],
                                                                      args["payload_min"],
                                                                      args["payload_max"],
                                                                      "true" if with_jemalloc else "false"))

def runBenchmarkBinary(binary, parameters, with_jemalloc, miliseconds=False):
    procString = "hwloc-bind node:1 " + os.path.join(os.getcwd(), "build", binary) + " " + " ".join([str(p) for p in parameters])
    if with_jemalloc:
        procString = "LD_PRELOAD=%s %s" % (JEMALLOC_PATH, procString)
    env = {"LD_LIBRARY_PATH": os.path.join(os.getcwd(), "..")}
    elapsed = 0.0
    for i in range(5):
        proc = subprocess.Popen(procString, shell=True, stdout=subprocess.PIPE, env=env)
        elapsed += float(proc.stdout.read())/5
    return elapsed/1000 if miliseconds else elapsed

def runBenchmark(binary, args, with_jemalloc=False):
    cachefile = getCacheFileName(binary, args, with_jemalloc)
    if not args["ignore_cached"] and os.path.isfile(cachefile):
        return eval(open(cachefile).read())
    result = []
    for numThreads in range(args["threads_min"], args["threads_max"]+1):
        result.append(runBenchmarkBinary(binary, [numThreads, args["payload_min"], args["payload_max"]], with_jemalloc, True))
    open(cachefile, "w").write(str(result))
    return result

def runRecovery(maxIterations, args):
    cachefile = getCacheFileName("bench_recovery", args, False)
    if not args["ignore_cached"] and os.path.isfile(cachefile):
        return eval(open(cachefile).read())
    result = []
    for numIterations in range(1, maxIterations+1):
        result.append(runBenchmarkBinary("bench_recovery", [numIterations, args["payload_min"], args["payload_max"]], False))
    open(cachefile, "w").write(str(result))
    return result

def plotBenchmark(benchname, args):
    fig = plt.figure()
    fig.set_size_inches(5.31, 3.54)
    plt.title(BENCHTITLES[benchname])
    plt.ylabel("Time in $ms$")
    plt.xlabel("Parallel Threads")
    plt.xlim(1, args["threads_max"])
    plotX = arange(1, args["threads_max"]+1)

    # default allocator
    print "Running '%s' for default malloc" % benchname
    plt.plot(plotX, runBenchmark("bench_%s" % benchname, args), label="default malloc", ls="--", color="black")

    # if selected, run with jemalloc
    if args["with_jemalloc"]:
        print "Running '%s' for jemalloc" % benchname
        plt.plot(plotX, runBenchmark("bench_%s" % benchname, args, True), label="jemalloc", ls=":", color="black")

    # run standard nvm_malloc
    print "Running '%s' for nvm_malloc" % benchname
    plt.plot(plotX, runBenchmark("bench_%s_nvm" % benchname, args), label="nvm\_malloc", ls="-", marker="o", color="black")

    # if selected, run nvm_malloc with CLFLUSHOPT
    if args["has_clflushopt"]:
        print "Running '%s' for nvm_malloc with CLFLUSHOPT" % benchname
        plt.plot(plotX, runBenchmark("bench_%s_nvm_clflushopt" % benchname, args), label="nvm\_malloc with CLFLUSHOPT", ls="-", marker="s", color="black")

    # if selected, run nvm_malloc with CLWB
    if args["has_clwb"]:
        print "Running '%s' for nvm_malloc with CLWB" % benchname
        plt.plot(plotX, runBenchmark("bench_%s_nvm_clwb" % benchname, args), label="nvm\_malloc with CLWB", ls="-", marker="v", color="black")

    # if selected, run nvm_malloc with fences disables
    if args["with_nofence"]:
        print "Running '%s' for nvm_malloc without fences" % benchname
        plt.plot(plotX, runBenchmark("bench_%s_nvm_nofence" % benchname, args), label="nvm\_malloc no fences", ls="-", marker="^", color="black")

    # if selected, run nvm_malloc with flushes disabled
    if args["with_noflush"]:
        print "Running '%s' for nvm_malloc without flushes" % benchname
        plt.plot(plotX, runBenchmark("bench_%s_nvm_noflush" % benchname, args), label="nvm\_malloc no flushes", ls="-", marker="+", color="black")

    # if selected, run nvm_malloc with both fences and flushes disabled
    if args["with_none"]:
        print "Running '%s' for nvm_malloc without flushes or fences" % benchname
        plt.plot(plotX, runBenchmark("bench_%s_nvm_none" % benchname, args), label="nvm\_malloc no fences/flushes", ls="-", marker="x", color="black")

    plt.legend(loc='upper left', prop={'size':10})
    plt.savefig(os.path.join(os.getcwd(), "plots", "%s.pdf" % benchname), dpi=1000, bbox_inches="tight")
    plt.close()

def plotRecoveryBenchmark(args):
    fig = plt.figure()
    fig.set_size_inches(5.31, 3.54)
    plt.title("nvm\_malloc internal recovery")
    plt.ylabel("Recovery time in $\mu s$")
    plt.xlabel("Iterations of 10k allocations")
    maxIterations = 20
    plt.xlim(1, maxIterations)
    plotX = arange(1, maxIterations+1)
    print "Running 'recovery' for nvm_malloc"
    plt.plot(plotX, runRecovery(maxIterations, args), color="black", ls="-")
    #plt.legend(loc='upper left', prop={'size':10})
    plt.savefig(os.path.join(os.getcwd(), "plots", "recovery.pdf"), dpi=1000, bbox_inches="tight")
    plt.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="nvm_malloc benchmarking tool")
    parser.add_argument("--run-all", action="store_true")
    parser.add_argument("--run-alloc-free", action="store_true")
    parser.add_argument("--run-alloc-free-alloc", action="store_true")
    parser.add_argument("--run-fastalloc", action="store_true")
    parser.add_argument("--run-linkedlist", action="store_true")
    parser.add_argument("--run-recovery", action="store_true")
    parser.add_argument("--threads-min", type=int, default=1)
    parser.add_argument("--threads-max", type=int, default=10)
    parser.add_argument("--payload-min", type=int, default=64)
    parser.add_argument("--payload-max", type=int, default=64)
    parser.add_argument("--has-clflushopt", action="store_true")
    parser.add_argument("--has-clwb", action="store_true")
    parser.add_argument("--with-jemalloc", action="store_true", help="include a run with jemalloc in the benchmark")
    parser.add_argument("--with-nofence", action="store_true", help="include a run with disabled fences")
    parser.add_argument("--with-noflush", action="store_true", help="include a run with disabled flushes")
    parser.add_argument("--with-none", action="store_true", help="include a run with disabled fences and flushes")
    parser.add_argument("--ignore-cached", action="store_true")
    args = vars(parser.parse_args())
    if args["payload_max"] < args["payload_min"]:
        args["payload_max"] = args["payload_min"]

    # make sure the cache and plot folders exists
    if not os.path.isdir("cached"):
        os.mkdir("cached")
    if not os.path.isdir("plots"):
        os.mkdir("plots")

    # run regular benchmarks
    for benchmark in BENCHMARKS:
        if args["run_%s" % benchmark] or args["run_all"]:
            plotBenchmark(benchmark, args)

    # run recovery benchmark
    if args["run_recovery"] or args["run_all"]:
        # set thread min/max to 0 to ignore variation for cache file
        args["threads_min"] = 0
        args["threads_max"] = 0
        plotRecoveryBenchmark(args)
