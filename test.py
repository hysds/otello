import otello
from pele_client.client import PeleRequests

if __name__ == "__main__":
    pele_base_url = "https://100.104.3.71/pele/api/v0.1"
    mozart_client = otello.mozart.Mozart()
    job_type = mozart_client.get_job_types()["job-SUBMIT_L2_HR_Raster:SSDS-2841"]
    job_type.initialize()
    pr = PeleRequests(pele_base_url, verify=False)
    r = pr.get(pele_base_url + "/pele/dataset/SWOT_L2_HR_PIXCVec_001_042_076R_20220402T112149_20220402T112159_PGA2_03")
    pixcvec_dataset = r.json()["result"]
    job_type.set_input_dataset(pixcvec_dataset)
