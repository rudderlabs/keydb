1. merge devops PR with new cluster size and new nodes addresses as well. we need a new "degraded nodes" variable
   that will specify which nodes should just sit there and not serve any traffic.
2. scaler can do the scaling and once done move the new nodes out of the "degraded state".
3. we merge a new devops PR to remove the "degraded" state permanently so that upon restart the nodes will still
   have the desired configuration.
