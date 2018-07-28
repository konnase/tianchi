#include <iostream>
#include <cstdlib>
#include <cstdio>
#include <cstring>
#include <vector>
#include <algorithm>
#include <numeric>
#include <string>
using namespace std;
const int maxn = 69000;
const double cpu_usage = 0.81;
const int cap = (int)92*cpu_usage;
const int small_cap = (int)32*cpu_usage;
const int large_bin_num = 3000;
double dp[cap+5]; // disk
int vis[maxn];
int g[maxn][cap+5];
struct inst {
	int cpu, mem, disk;
	char id[30];
} a[maxn];
bool cmp(inst x, inst y) {
    return x.cpu > y.cpu;
}
int value(int cpu, int mem, int disk) {
    return disk;
}
int main() {
    int n;
    scanf("%d", &n);
    for(int i = 0; i < n; i++) {
        scanf("%d %d %d %s", &a[i].cpu, &a[i].mem, &a[i].disk, a[i].id);
    }
    sort(a, a+n, cmp);  // instances are sorted by cpu size

    int ans = 0;
    int cpu_cap = cap;
    int unpacked = n;
    vector<int> vis(n, 1);

    while(unpacked > 0) {
        ans ++;
        memset(dp, 0, sizeof(dp));
        memset(g, 0, sizeof(g));

        vector<int> bin;

        int first_undeployed = -1;
        int large_undeployed = 0;
        for(int i = 0; i < n; i++) {
            if (vis[i] && a[i].disk > small_cap) {
                first_undeployed = i;
                large_undeployed += 1;
            }
        }
        if (large_bin_num - large_undeployed == ans) {
            bin.push_back(first_undeployed);
            cpu_cap -= a[first_undeployed].cpu;
        }
        for(int i = 0; i < n; i++) {
            if (vis[i] == 0) continue;
            for(int k = cpu_cap; k >= a[i].cpu; k--) {
                if(dp[k - a[i].cpu] + value(a[i].cpu, a[i].mem, a[i].cpu) > dp[k]) {
                    dp[k] = dp[k - a[i].cpu] + value(a[i].cpu, a[i].mem, a[i].cpu);
                    g[i][k] = 1; // allocate k units cpu to instance i
                }
            }
        }

        if (dp[cpu_cap] > 0) {
            int y = cpu_cap;
            for(int i = n - 1; i >= 0 &&  y >= 0; i--) {
                if (g[i][y]) {
                    y -= a[i].cpu;
                    vis[i] = 0; // instance i has been packed
                    bin.push_back(i);
                }
            }

            printf("total(%.2f): {", dp[cpu_cap]);
            for(int i = 0; i < bin.size(); i++) {
                printf("%d", a[bin[i]].cpu);
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

            if(ans == large_bin_num) cpu_cap = small_cap;

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
