#include <iostream>
#include <cstdio>
#include <cstring>
#include <vector>
#include <algorithm>
#include <numeric>
#include <string>
using namespace std;
const int maxn = 69000;
double dp[1100]; // disk
int vis[maxn];
int g[maxn][1100];
struct inst {
	int cpu, mem, disk;
	char id[30];
} a[maxn];
bool cmp(inst x, inst y) {
    return x.disk > y.disk;
}
double value(int cpu, int mem, int disk) {
    double w_cpu, w_mem, w_disk;
    if (disk > 600) {
        w_cpu = -0.1;
        w_mem = 0.1;
        w_disk = 7;
    } else {
        w_cpu = -0.5;
        w_mem = 0.3;
        w_disk = 4;
    }
    return w_cpu * cpu + w_mem * mem + w_disk * disk;
}
int main() {
    int n;
    scanf("%d", &n);
    for(int i = 0; i < n; i++) {
        scanf("%d %d %d %s", &a[i].cpu, &a[i].mem, &a[i].disk, a[i].id);
    }
    sort(a, a+n, cmp);  // instances are sorted by disk size

    int ans = 0;
    int disk_cap = 1024;
    int unpacked = n;
    vector<int> vis(n, 1);

    while(unpacked > 0) {
        ans ++;
        memset(dp, 0, sizeof(dp));
        memset(g, 0, sizeof(g));
        for(int i = 0; i < n; i++) {
            if (vis[i] == 0) continue;
            for(int k = disk_cap; k >= a[i].disk; k--) {
                if(dp[k - a[i].disk] + value(a[i].cpu, a[i].mem, a[i].disk) > dp[k]) {
                    dp[k] = dp[k - a[i].disk] + value(a[i].cpu, a[i].mem, a[i].disk);
                    g[i][k] = 1; // allocate k units disk to instance i
                }
            }
        }

        if (dp[disk_cap] > 0) {
            int y = disk_cap;
            vector<int> bin;
            for(int i = n - 1; i >= 0 &&  y >= 0; i--) {
                if (g[i][y]) {
                    y -= a[i].disk;
                    vis[i] = 0; // instance i has been packed
                    bin.push_back(i);
                }
            }

            printf("total(%.2f): {", dp[disk_cap]);
            for(int i = 0; i < bin.size(); i++) {
                printf("%d", a[bin[i]].disk);
                if (i == bin.size() - 1) {
                    printf("}");
                } else {
                    printf(",");
                }
            }
            printf(" (");
            for(int i = 0; i < bin.size(); i++) {
                printf("%s", a[bin[i]].id);
                if (i == bin.size() - 1) {
                    printf(")\n");
                } else {
                    printf(",");
                }
            }

            if(ans == 3000) disk_cap = 600;

            unpacked = accumulate(vis.begin(), vis.end(), 0);
        } else {
            break;
        }
    }
    if(unpacked > 0) {
        printf("undeployed: ");
        for(int i = 0; i < n; i++) {
            if(vis[i]) printf("%s,", a[i].id);
        }
        printf("\n");
    }
}
