- **WSL**
```aiignore
redis-cli -h $(ip route show | grep -i default | awk '{ print $3}')
redis-cli -h 172.18.160.1
```