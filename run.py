from AlgorithmA import MatrixMultiplication

if __name__ == '__main__':
    mr_job = MatrixMultiplication(['--no-conf', '--runner=local', 'File1ForLab3.txt'])
    with mr_job.make_runner() as runner:
        tmp_output = []
        runner.run()
        for line in runner.stream_output():
            tmp_output = tmp_output + [line]