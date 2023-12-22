"""The HEFT(Heterogeneous Earliest Finish Time) Scheduling Algorithm of DAGs"""
import math
import operator
import copy
import time


class Ippts:

    def __init__(self, q, n, v):
        """"""
        self.Q = q   # Number of processsors
        self.n = n  # The DAG to schedule
        self.v = v  # the number of task
        self.dag = {}
        self.computation_costs = [] # Computation Cost Table
        self.Pi = {}
        self.scheduler = []  # Recording the scheduling processors'id of different tasks.
        self.avg_costs = []  # Computing the average computation costs of every task.
        self.pred = []  # Predecessor node list
        self.rank_pcm = [] # PMC matrix
        self.prank = []
        self.pcm_table = {}

    def read_dag(self):
        """q is the number of processors, n is which DAG"""
        # dag_ = {}
        filename = 'dags' + '/' + 'n=' + str(self.v) + 'q=' + str(self.Q) + '/_' + str(self.n) + '_dag_q=' + str(self.Q) + '.txt'
        """
        """
        with open(filename, 'r') as file_object_:
            lines = file_object_.readlines()
            task_id = 0
            for line in lines:
                line_list = line.split()
                task_id = int(line_list[0])
                succ_dict = {}
                for line_ in lines:
                    line_list_ = line_.split()
                    task_id_ = int(line_list_[0])
                    succ_id_ = int(line_list_[1])
                    succ_weight = float(line_list_[2])
                    if task_id == task_id_:
                        succ_dict[succ_id_] = succ_weight
                self.dag[task_id] = succ_dict
            self.dag[task_id + 1] = {}
        return self.dag

    def read_computation_costs(self):
        """q is the number of processors, n is which graph"""
        filename = 'dags' + '/' + 'n=' + str(self.v) + 'q=' + str(self.Q) + '/_' + str(self.n) \
                   + '_computation_costs_q=' + str(self.Q) + '.txt'
        with open(filename, 'r') as file_object:
            lines = file_object.readlines()
            for line in lines:
                line_list = line.split()
                temp_list = []
                for i in range(len(line_list)):
                    temp_list.append(float(line_list[i]))
                self.computation_costs.append(temp_list)
        return self.computation_costs

    def get_dag_costs(self):
        self.read_dag()
        self.read_computation_costs()
    def prank_(self):
        """Computing the task  priorities"""
        self.get_dag_costs()
        dag = self.dag
        v = self.v
        rank_pcm_list = {}
        listt = []
        task_succ_list = {}

        for k in range(self.v):
            for succ_task in self.dag[k+1].keys():
                if len(dag[k+1]) == 0:
                    listt.append(0)
                else:
                    listt.append(succ_task)
                break
            task_succ_list[k+1] = listt

        while v > 0:
            if len(dag[v]) == 0:  # Task with no successors
                self.pcm_table[v] = list([self.computation_costs[v - 1][i] for i in range(self.Q)])
                rank_pcm_list[v] = (sum(self.pcm_table[v]) / self.Q) * len(dag[v])
                self.rank_pcm.append([v,  rank_pcm_list[v]])

            else:  # have successors
                # Finding subsequent nodes j
                pcm_table = []
                for pk in range(1, self.Q + 1):
                    max_pcm = 0
                    max_succ = -1
                    max_succ_task = -1
                    for tj in self.dag[v].keys():
                        """tj is succ of ti"""
                        min_pcm = math.inf
                        for pw in range(1, self.Q + 1):
                            pcm_tj_pw = self.pcm_table[tj][pw - 1]
                            """find w(tj, pw)"""
                            w_tj_pw = self.computation_costs[tj - 1][pw - 1]
                            w_ti_pw = self.computation_costs[v - 1][pw - 1] # v=ti

                            """find avg_ci,j"""
                            if pw == pk:
                                avg_cij = 0
                            else:
                                avg_cij = self.dag[v][tj]
                            sum_pcm = (pcm_tj_pw + w_tj_pw + w_ti_pw + avg_cij)
                            if min_pcm > sum_pcm:
                                min_pcm = sum_pcm
                        if max_pcm < min_pcm:
                            max_pcm = min_pcm

                        if max_succ < len(self.dag[tj]):
                            max_succ = len(self.dag[tj])
                            max_succ_task = tj
                    pcm_table.append(max_pcm)

                self.pcm_table[v] = pcm_table
                avg_pcm = round((sum(pcm_table) / self.Q), 2)
                rank_pcm_list[v] = avg_pcm * len(dag[v])

                if rank_pcm_list[v] < rank_pcm_list[max_succ_task]:
                    rank_pcm_list[v] = rank_pcm_list[max_succ_task] + 1 / (self.v * max_succ)
                self.rank_pcm.append([v, rank_pcm_list[v]])
            v -= 1
        print("PCM table", self.pcm_table)

        return self.rank_pcm, self.pcm_table

    def execute_rank__u(self):
        self.prank_()
        self.rank_pcm.sort(key=operator.itemgetter(1), reverse=True)
        self.prank = copy.deepcopy(self.rank_pcm)
        print("Prank", self.prank)
        return self.prank

    def pred_list(self):
        """Finding the Predecessor Node"""
        dag = self.dag
        self.execute_rank__u()

        for m in range(len(self.rank_pcm)):
            job_ = self.rank_pcm[m][0]
            temp = []
            for j in range(len(dag)):
                if job_ in dag[j + 1].keys():
                    sub_pred = j + 1
                    temp.append(sub_pred)
            self.pred.append([job_, temp])

        return self.pred

    def add_pi(self, pi_, job_, est_, eft_):
        """Join the task to the list and add the task to the schedule list"""
        list_pi = []
        if pi_ in self.Pi.keys():
            list_pi = list(self.Pi[pi_])
            list_pi.append({'job': job_, 'est': est_, 'end': eft_})
            self.Pi[pi_] = list_pi
            self.scheduler.append({job_: pi_})
        else:
            list_pi.append({'job': job_, 'est': est_, 'end': eft_})
            self.Pi[pi_] = list_pi
            self.scheduler.append({job_: pi_})

    def get_aft(self, job_pred_j):
        """"""
        aft = 0
        pred_pi = 0
        for k in range(len(self.scheduler)):  # rank_u_copy
            if job_pred_j == self.prank[k][0]:
                pred_pi = self.scheduler[k][job_pred_j]
                aft = 0
                for m in range(len(self.Pi[pred_pi])):
                    if self.Pi[pred_pi][m]['job'] == job_pred_j:
                        aft = self.Pi[pred_pi][m]['end']
        return aft, pred_pi

    def pred_max_nm(self, pi_, job_pred_, job_):
        """The maximum time spent on a precursor node."""
        dag = self.dag

        max_nm_ = 0
        for j in range(len(job_pred_)):
            # print(job_pred_[j])
            # Finding the completion time of the predecessor.1）Finding which processor the predecessor is on
            job_pred_j = job_pred_[j]
            """get aft"""
            aft, pred_pi = self.get_aft(job_pred_j)
            # computing cmi
            if pi_ == pred_pi:
                cmi = 0
            else:
                cmi = dag[job_pred_[j]][job_]
            if max_nm_ < aft + cmi:
                max_nm_ = aft + cmi
        return max_nm_


    def ippts(self):
        """"""
        self.start_time = time.time()
        self.pred_list()
        computation_costs = self.computation_costs
        v = self.v

        while len(self.rank_pcm) > 0:
            """Select the first task schedule in the list each time"""
            job = self.rank_pcm.pop(0)[0]  # task id

            if len(self.rank_pcm) == v - 1:  # The first task
                label_pi = 1
                label_est = 0
                min_o_eft = math.inf

                for pi in range(len(computation_costs[0])):
                    eft = self.computation_costs[job - 1][pi]
                    LDET = self.pcm_table[job][pi] - self.computation_costs[job - 1][pi]
                    o_eft = eft + LDET
                    if min_o_eft > o_eft:
                        min_o_eft = o_eft
                        label_pi = pi + 1
                label_eft = self.computation_costs[job - 1][label_pi - 1]
                self.add_pi(label_pi, job, label_est, label_eft)
                print([job, label_est, label_eft, label_pi])

            else:  # other tasks
                """First computing max(n_m∈pred(n_i)){AFT(n_m ) + c_(m,i)}"""
                label_pi = 0
                label_est = 0
                label_eft = 0
                min_o_eft = math.inf
                for pi in range(1, self.Q + 1):  # Scheduling on different processors.
                    est = 0
                    max_nm = 0
                    for i in range(len(self.pred)):
                        if job == self.pred[i][0]:  # Find the index position of predecessor i
                            job_pred = self.pred[i][1]
                            max_nm = self.pred_max_nm(pi, job_pred, job)

                    # Computing the earliest time that processor can handle of task job.
                    avail_pi = 0
                    if pi in self.Pi.keys():
                        avail_pi = self.Pi[pi][-1]['end']

                    """get est"""
                    if est < max(avail_pi, max_nm):
                        est = max(avail_pi, max_nm)
                    """get eft"""
                    eft = est + self.computation_costs[job - 1][pi - 1]
                    """get o_eft"""
                    LDET = self.pcm_table[job][pi - 1] - self.computation_costs[job - 1][pi - 1]

                    o_eft = eft + LDET

                    if min_o_eft > o_eft:
                        min_o_eft = o_eft
                        label_pi = pi
                        label_est = est
                        label_eft = eft

                self.add_pi(label_pi, job, label_est, label_eft)
                print([job, label_est, label_eft, label_pi]) # task, est, eft, selected processor

        makespan = label_eft

        return makespan

if __name__ == "__main__":
    q = 2  # number of processor
    n = 1  # dag ID
    v = 6  # Number of tasks
    ippts = Ippts(q, n, v)
    make_span = ippts.ippts()

    print("-----------------------IPPTS-----------------------")
    print('make_span =', make_span)
    print("-----------------------IPPTS-----------------------")
