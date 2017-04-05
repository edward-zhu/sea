from master.job import Job
import unittest

class TestJobBasic(unittest.TestCase):
    def test_job_create(self):
        f = open('job1.yaml', 'r')
        j = Job("", "test", f)
        print(j.desc())
        for _, t in j._tasks.items():
            print(t.new_task_req())
        f.close()

if __name__ == '__main__':
    unittest.main()