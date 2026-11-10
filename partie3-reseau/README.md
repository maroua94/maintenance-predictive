# Partie 3 — Réseau & Collecte (Marwa)

## Topologie GNS3 (SCRUM-15)

![Topologie](gns3/topologie.png)

- 3 routeurs Cisco c7200 (R1, R2, R3) reliés en triangle, routage OSPF zone 0
- Réseau de gestion séparé (192.168.10.0/24) relié à la VM du collecteur via VMnet2 (VMware, host-only, sans DHCP)

| Lien | Réseau | Adresses |
|---|---|---|
| Gestion (f0/0) | 192.168.10.0/24 | R1 .1 · R2 .2 · R3 .3 · collecteur .100 |
| R1 f1/0 – R2 f1/0 | 10.0.12.0/30 | R1 .1 · R2 .2 |
| R2 f1/1 – R3 f1/0 | 10.0.23.0/30 | R2 .1 · R3 .2 |
| R3 f1/1 – R1 f1/1 | 10.0.13.0/30 | R1 .1 · R3 .2 |
| Loopback0 | — | 1.1.1.1 · 2.2.2.2 · 3.3.3.3 |

Les configurations des routeurs sont dans `gns3/R1.cfg`, `gns3/R2.cfg`, `gns3/R3.cfg`.
