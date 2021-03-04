#ifndef MAINCLASS_H
#define MAINCLASS_H

#include <QObject>
#include <QTimer>
#include <QtConcurrent>

#include <QSqlDatabase>
#include <QSqlQuery>
#include <QSqlQueryModel>
#include <QSqlRecord>
#include <QSqlError>

#include <QtWebSockets/QtWebSockets>
#include <QMutex>
#include <QDataStream>

#include "KGEEWLIBS_global.h"
#include "kgeewlibs.h"

#define QSCD1002WSOCK_VERSION 0.1

class MainClass : public QObject
{
    Q_OBJECT
public:
    explicit MainClass(QString conFile = nullptr, QObject *parent = nullptr);

private:
    _CONFIGURE cfg;
    RecvQSCD100Message *rvQSCD100_Thread;

    QMutex mutex;

    // About Database & table
    QSqlDatabase qscdDB;
    QSqlQueryModel *networkModel;
    QSqlQueryModel *affiliationModel;
    QSqlQueryModel *siteModel;

    void openDB();
    void readStationListfromDB();

    projPJ pj_eqc;
    projPJ pj_longlat;
    void initProj();
    void initPoints();

    QWebSocketServer *m_pWebSocketServer;
    QList<QWebSocket *> m_clients;

private slots:
    void doRepeatWork();
    void extractQSCD(QMultiMap<int, _QSCD_FOR_MULTIMAP>);
    void onNewConnection();
    void socketDisconnected();
};



class ProcessQSCDThread : public QThread
{
    Q_OBJECT
public:
    ProcessQSCDThread(QWebSocket *websocket = nullptr, QWidget *parent = nullptr);
    ~ProcessQSCDThread();

public slots:
    void genData(QString);

private:
    QWebSocket *pSocket;
    void fillPGAintoStaList_Parallel(QList<_QSCD_FOR_MULTIMAP>, int);
    QByteArray generateData(int, int, int);

    void sendBinaryMessage(QByteArray);
};
#endif // MAINCLASS_H
