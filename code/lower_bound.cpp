#include <iostream>
#include <cstdio>
#include <cstring>
#include <vector>
#include <algorithm>
#include <numeric>
using namespace std;
const int maxn = 70000;
int dp[maxn];
int a[maxn];
int vis[maxn];
int g[maxn][1500];
bool cmp(int x, int y) { return x > y; }
int main() {
    int n;
    scanf("%d", &n);
    for(int i = 0; i < n; i++) {
        scanf("%d", &a[i]);
    }
    sort(a, a+n, cmp);

    int ans = 0;
    int cap = 1024;
    int unpacked = n;
    vector<int> vis(n, 1);

    while(unpacked > 0) {
        ans ++;
        memset(dp, 0, sizeof(dp));
        memset(g, 0, sizeof(g));
        for(int i = 0; i < n; i++) {
            if (vis[i] == 0) continue;
            for(int j = cap; j >= a[i]; j--) {
                if(dp[j - a[i]] + a[i] > dp[j]) {
                    dp[j] = dp[j-a[i]] + a[i];
                    g[i][j] = 1;
                }
            }
        }

        int x = cap;
        vector<int> bin;
        for(int i = n - 1; i >= 0 && x >= 0; i--) {
            if (g[i][x]) {
                x -= a[i];
                vis[i] = 0;
                bin.push_back(a[i]);
            }
        }

        printf("total(%d): {", dp[cap]);
        for(int i = 0; i < bin.size(); i++) {
            printf("%d", bin[i]);
            if (i == bin.size() - 1) {
                printf("}\n");
            } else {
                printf(",");
            }
        }

        if(ans == 3000) cap = 600;

        unpacked = accumulate(vis.begin(), vis.end(), 0);
        if(dp[cap] == 0) break;
    }
    if(unpacked > 0) {
        for(int i = 0; i < n; i++) {
            if(vis[i]) printf("%d ", a[i]);
        }
        printf("\n");
    }
}
