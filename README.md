# Acknowledgments
This project has been funded by the European Unions Horizon 2020 research and innovation programme under Grant Agreement no. 732366 (ACTiCLOUD).

# MDBconductor
This repository contains the implementation of MDBconductor.

An MDBconductor conducts a cluster of MonetDB VM instances in the cloud.
Upon receiving a user query, it dispatches based on some criteria the query to
 one of the VMs in its reign for exeuction.
For instance, it can make an estimation for the memory footprint of this query,
 and use this information to select the most suitable VM to execute this query.
Another criteria can be the load on the VMs.

If none of the existing VMs is suitable for the incoming query, MDBconductor
 will first resize an existing VM (to satisfy the required hardware resource),
 or start a new VM (to avoid overloading the existing VMs).

