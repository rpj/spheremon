#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <stdint.h>
#include <errno.h>
#include <string.h>
#include <time.h>
#include <netdb.h>
#include <sys/types.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <assert.h>

#include <applibs/networking.h>
#include <applibs/gpio.h>

#include <yarl.h>

// adapted from http://beej.us/guide/bgnet/html/multi/clientserver.html#simpleclient

// get sockaddr, IPv4 or IPv6:
void *get_in_addr(struct sockaddr *sa)
{
    if (sa->sa_family == AF_INET)
    {
        return &(((struct sockaddr_in *)sa)->sin_addr);
    }

    return &(((struct sockaddr_in6 *)sa)->sin6_addr);
}

RedisConnection_t RedisConnect(const char *host, const char *port)
{
    int sockfd;
    struct addrinfo hints, *servinfo, *p;
    int rv;
    char s[INET6_ADDRSTRLEN];

    memset(&hints, 0, sizeof hints);
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;

    if ((rv = getaddrinfo(host, port, &hints, &servinfo)) != 0)
    {
        fprintf(stderr, "getaddrinfo: %s\n", gai_strerror(rv));
        return -1;
    }

    // loop through all the results and connect to the first we can
    for (p = servinfo; p != NULL; p = p->ai_next)
    {
        if ((sockfd = socket(p->ai_family, p->ai_socktype, p->ai_protocol)) == -1)
        {
            perror("client: socket");
            continue;
        }

        if (connect(sockfd, p->ai_addr, p->ai_addrlen) == -1)
        {
            close(sockfd);
            perror("client: connect");
            continue;
        }

        break;
    }

    if (p == NULL)
    {
        fprintf(stderr, "client: failed to connect\n");
        return -2;
    }

    inet_ntop(p->ai_family, get_in_addr((struct sockaddr *)p->ai_addr), s, sizeof s);
    freeaddrinfo(servinfo); // all done with this structure
    return (RedisConnection_t)sockfd;
}

#define WAIT_FOR_WIFI_SECONDS   120

bool netCheck(void);

bool netCheck()
{
    bool netUp = false;
    return Networking_IsNetworkingReady(&netUp) != -1 && netUp;
}

int checkKeys(RedisConnection_t conn, RedisArray_t *keys)
{
    assert(keys && keys->count);
    int lostCount = 0;
    for (int i = 0; i < keys->count; i++)
    {
        char *checkKey = (char*)keys->objects[i].obj;
        if (!Redis_EXISTS(conn, checkKey))
        {
            printf("Lost '%s'!\n", checkKey);
            ++lostCount;
        }
    }
    return lostCount;
}

#define RED_LED 8
#define GREEN_LED 9
#define BLUE_LED 10
#define RED_FDIDX (RED_LED-RED_LED)
#define GREEN_FDIDX (GREEN_LED-RED_LED)
#define BLUE_FDIDX (BLUE_LED-RED_LED)
#define LED_COUNT 3
#define LED_ON GPIO_Value_Low
#define LED_OFF GPIO_Value_High

int* setupLEDs(void);
int* setupLEDs()
{
    // keeps these in index order from above or else!
    int leds[LED_COUNT] = { RED_LED, GREEN_LED, BLUE_LED };
    int* fds = (int*)malloc(LED_COUNT * sizeof(int));

    for (int i = 0; i < LED_COUNT; i++) {
        fds[i] = GPIO_OpenAsOutput(leds[i], GPIO_OutputMode_PushPull, GPIO_Value_High);
        if (fds[i] < 0) {
            fprintf(stderr,
                "Error opening GPIO %u: %s (%d). Check that app_manifest.json includes the GPIO used.\n",
                leds[i], strerror(errno), errno);
            return NULL;
        }
    }
    return fds;
}

#define TOGGLE_ALL(fds, val) do { for (int i = 0; i < LED_COUNT; i++) GPIO_SetValue(fds[i], val); } while(0)

int main(int argc, char** argv)
{
    if (argc < 3)
    {
        fprintf(stderr, "Usage: %s host port password\n\n", argv[0]);
        exit(-1);
    }

    const char* host = argv[1];
    const char* port = argv[2];
    const char* pass = argc > 3 ? argv[3] : NULL;

    int* fds = setupLEDs();

    if (!fds)
    {
        fprintf(stderr, "LED setup failed\n\n");
        exit(-1);
    }

    GPIO_SetValue(fds[BLUE_FDIDX], LED_ON);

    const struct timespec sleepTime = { 1, 0 };
    static int netCheckRetries = WAIT_FOR_WIFI_SECONDS;
    while (!netCheck() && --netCheckRetries)
    {
        const struct timespec quickTime = { 0, 5e7 };
        GPIO_SetValue(fds[RED_FDIDX], LED_ON);
        nanosleep(&quickTime, NULL);
        GPIO_SetValue(fds[RED_FDIDX], LED_OFF);
        nanosleep(&sleepTime, NULL);
    }

    GPIO_SetValue(fds[BLUE_FDIDX], LED_OFF);

    if (!netCheckRetries)
    {
        perror("Networking init failed");
        exit(-errno);
    }

    GPIO_SetValue(fds[GREEN_FDIDX], LED_ON);

    RedisConnection_t rConn = RedisConnect(host, port);

    if (rConn < 1)
    {
        fprintf(stderr, "RedisConnect failed: %d\n", rConn);
        exit(rConn);
    }

    if (pass && !Redis_AUTH(rConn, pass))
    {
        fprintf(stderr, "AUTH failed\n");
        exit(-1);
    }

    RedisArray_t *rpjiosKeys = Redis_KEYS(rConn, "rpjios.checkin.*");
    RedisArray_t *zerowatchKeys = Redis_KEYS(rConn, "*:heartbeat");

    if (!rpjiosKeys || !zerowatchKeys)
    {
        fprintf(stderr, "Failed to query key sets we expected\n");
        exit(-2);
    }

    const struct timespec loopTime = { 10, 0 };
    const struct timespec blinkTime = { 1, 0 };

    printf("Found %d rpjios and %d zerowatch keys to monitor every %ds...\n",
        rpjiosKeys->count, zerowatchKeys->count, loopTime.tv_sec);

    nanosleep(&blinkTime, NULL);
    GPIO_SetValue(fds[GREEN_FDIDX], LED_OFF);
    int lastLost = 0;

    while (1)
    {
        printf("Running key check...\n");
        if (!lastLost)
        {
            const struct timespec quickTime = { 0, 3e7 };
            for (int i = 0; i < LED_COUNT; i++)
            {
                GPIO_SetValue(fds[i], LED_ON);
                nanosleep(&quickTime, NULL);
                GPIO_SetValue(fds[i], LED_OFF);
                nanosleep(&quickTime, NULL);
            }
        }

        lastLost = checkKeys(rConn, rpjiosKeys) + checkKeys(rConn, zerowatchKeys);
        if (lastLost)
        {
            printf("Lost %d keys!\n", lastLost);
            GPIO_SetValue(fds[RED_FDIDX], LED_ON);
            for (int i = 0; i < lastLost; i++)
            {
                GPIO_SetValue(fds[BLUE_FDIDX], LED_ON);
                nanosleep(&blinkTime, NULL);
                GPIO_SetValue(fds[BLUE_FDIDX], LED_OFF);
                nanosleep(&blinkTime, NULL);
            }
        }
        else
        {
            TOGGLE_ALL(fds, LED_OFF);
        }

        fflush(stdout);
        fflush(stderr);
        nanosleep(&loopTime, NULL);
    }
}