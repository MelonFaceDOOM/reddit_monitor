from analysis.acip import refresh_acip_analysis
from vsm import init_connection

if __name__ == "__main__":
    init_connection()  # sets up ssh_tunnel and pg_pool
    refresh_acip_analysis()
