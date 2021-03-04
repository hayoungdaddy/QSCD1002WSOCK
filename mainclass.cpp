#include "mainclass.h"

#include "landXY_VERSION_1.h"

QList<_QSCD_FOR_MULTIMAP> prexPgaList;
int prexDataEpochTime;
double maxdist;
int monChanID;

QMultiMap<int, _QSCD_FOR_MULTIMAP> QSCD_DATA_HOUSE;
QList<_STATION> prexStaList;
QList<_STATION> staticStaList;
QList<_STATION> tempStaList;

QList<_POINT> points;

static void getMapZValue(_POINT &point)
{
    double A=0., B=0., z=0., W=0.;
    int i;

    point.staList = tempStaList;

    for(i=0;i<point.staList.size();i++)
    {
        if(point.staList.at(i).pga == 0) continue;

        if(point.landX == point.staList.at(i).smapX && point.landY == point.staList.at(i).smapY)
        {
            A = point.staList.at(i).pga[monChanID] ;
            B = 1 ;
            break ;
        }

        z = point.staList.at(i).pga[monChanID] ;
        W = point.mapLUT.at(i);

        A  += ( W * z ) ;
        B  += ( W ) ;
    }
    point.mapZ = A/B;
};

static bool init_MSM_parallel()
{
    maxdist = 0;
    for(int i=0;i<prexStaList.size();i++)
    {
        for(int j=0;j<prexStaList.size();j++)
        {
            _STATION sta1 = prexStaList.at(i);
            _STATION sta2 = prexStaList.at(j);
            double d = sqrt(pow((double)(sta1.smapX - sta2.smapX), 2) +
                            pow((double)(sta1.smapY - sta2.smapY), 2));
            if(maxdist < d)
                maxdist = d;
        }
    }

    std::function<void(_POINT&)> getMapLut = [](_POINT &point)
    {
        double Rw = 0, Rp = 0 ;

        int Np =  9 ;
        int Nw = 18 ;

        Rw = maxdist / 2 * sqrt((double)Nw / (double)prexStaList.size());
        Rp = maxdist / 2 * sqrt((double)Np / (double)prexStaList.size());

        double distan, dis, W;

        if(!point.mapLUT.isEmpty())
            point.mapLUT.clear();

        for(int i=0;i<prexStaList.size();i++)
        {
            _STATION mysta = prexStaList.at(i);
            distan = sqrt(pow((double)(point.landX - mysta.smapX), 2) + pow((double)(point.landY - mysta.smapY), 2) );
            dis = Rp - distan;
            W = 0 ;

            if(dis >= 0)
                W  = pow( (dis / (Rp * distan)), 2) ;
            else if(dis < 0)
                W = pow( (1 / (Rp * distan)), 2) ;

            point.mapLUT.append(W);
        }
    };

    QFuture<void> future = QtConcurrent::map(points, getMapLut);
    future.waitForFinished();

    return true;
}

static void initIDW()
{
    if( !init_MSM_parallel() )
    {
        qDebug() << "Can't initialize MSM. Exit.";
        exit(1);
    }
}

MainClass::MainClass(QString configFileName, QObject *parent) : QObject(parent)
{
    activemq::library::ActiveMQCPP::initializeLibrary();

    cfg = readCFG(configFileName);
    qRegisterMetaType< QMultiMap<int, _QSCD_FOR_MULTIMAP> >("QMultiMap<int,_QSCD_FOR_MULTIMAP>");

    initProj();
    initPoints();

    writeLog(cfg.logDir, cfg.processName, "======================================================");

    qscdDB = QSqlDatabase::addDatabase("QMYSQL");
    qscdDB.setHostName(cfg.db_ip);
    qscdDB.setDatabaseName(cfg.db_name);
    qscdDB.setUserName(cfg.db_user);
    qscdDB.setPassword(cfg.db_passwd);

    networkModel = new QSqlQueryModel();
    affiliationModel = new QSqlQueryModel();
    siteModel = new QSqlQueryModel();

    readStationListfromDB();
    initIDW();

    QSCD_DATA_HOUSE.clear();

    QTimer *systemTimer = new QTimer;
    connect(systemTimer, SIGNAL(timeout()), this, SLOT(doRepeatWork()));
    systemTimer->start(1000);

    // consumer
    if(cfg.qscd100_amq_topic != "")
    {
        QString failover = "failover:(tcp://" + cfg.qscd100_amq_ip + ":" + cfg.qscd100_amq_port + ")";

        rvQSCD100_Thread = new RecvQSCD100Message;
        if(!rvQSCD100_Thread->isRunning())
        {
            rvQSCD100_Thread->setup(failover, cfg.qscd100_amq_user, cfg.qscd100_amq_passwd,
                                         cfg.qscd100_amq_topic, true, false);
            connect(rvQSCD100_Thread, SIGNAL(sendQSCDtoMain(QMultiMap<int, _QSCD_FOR_MULTIMAP>)),
                    this, SLOT(extractQSCD(QMultiMap<int, _QSCD_FOR_MULTIMAP>)));
            rvQSCD100_Thread->start();
        }
    }

    writeLog(cfg.logDir, cfg.processName, "QSCD1002WSOCK Started");


    m_pWebSocketServer = new QWebSocketServer(QStringLiteral("QSCD1002WSOCK") + cfg.processName,
                                              QWebSocketServer::NonSecureMode,  this);

    if(m_pWebSocketServer->listen(QHostAddress::Any, cfg.websocketPort))
    {
        writeLog(cfg.logDir, cfg.processName, "Listening on port : " + QString::number(cfg.websocketPort));

        connect(m_pWebSocketServer, &QWebSocketServer::newConnection,
                this, &MainClass::onNewConnection);
        connect(m_pWebSocketServer, &QWebSocketServer::closed,
                this, &QCoreApplication::quit);
    }
}

void MainClass::onNewConnection()
{
    QWebSocket *pSocket = m_pWebSocketServer->nextPendingConnection();
    connect(pSocket, &QWebSocket::disconnected, this, &MainClass::socketDisconnected);
    m_clients << pSocket;

    ProcessQSCDThread *prThread = new ProcessQSCDThread(pSocket);
    if(!prThread->isRunning())
    {
        prThread->start();
        connect(pSocket, &QWebSocket::disconnected, prThread, &ProcessQSCDThread::quit);
        connect(pSocket, &QWebSocket::textMessageReceived, prThread, &ProcessQSCDThread::genData);
        connect(prThread, &ProcessQSCDThread::finished, prThread, &ProcessQSCDThread::deleteLater);
    }
}

void MainClass::socketDisconnected()
{
    QWebSocket *pClient = qobject_cast<QWebSocket *>(sender());

    if(pClient){
        m_clients.removeAll(pClient);
        pClient->deleteLater();
    }
}

void MainClass::extractQSCD(QMultiMap<int, _QSCD_FOR_MULTIMAP> mmFromAMQ)
{
    QMapIterator<int, _QSCD_FOR_MULTIMAP> i(mmFromAMQ);

    mutex.lock();
    while(i.hasNext())
    {
        i.next();
        QSCD_DATA_HOUSE.insert(i.key(), i.value());
    }
    mutex.unlock();
}

void MainClass::doRepeatWork()
{
    QDateTime systemTimeUTC = QDateTime::currentDateTimeUtc();
    QDateTime dataTimeUTC = systemTimeUTC.addSecs(- SECNODS_FOR_ALIGN_QSCD); // GMT

    mutex.lock();

    if(!QSCD_DATA_HOUSE.isEmpty())
    {
        QMultiMap<int, _QSCD_FOR_MULTIMAP>::iterator iter;
        QMultiMap<int, _QSCD_FOR_MULTIMAP>::iterator untilIter;
        untilIter = QSCD_DATA_HOUSE.lowerBound(dataTimeUTC.toTime_t() - KEEP_LARGE_DATA_DURATION);

        for(iter = QSCD_DATA_HOUSE.begin() ; untilIter != iter;)
        {
            QMultiMap<int, _QSCD_FOR_MULTIMAP>::iterator thisIter;
            thisIter = iter;
            iter++;
            QSCD_DATA_HOUSE.erase(thisIter);
        }
    }
    mutex.unlock();
}


void MainClass::initPoints()
{
    points.clear();

    for(int i=0;i<LANDXYCNT_VERSION_1;i++)
    {
        _POINT mypoint;
        mypoint.landX = landXY_Version_1[i][0];
        mypoint.landY = landXY_Version_1[i][1];

        points.append(mypoint);
    }
}

void MainClass::openDB()
{
    if(!qscdDB.isOpen())
    {
        if(!qscdDB.open())
        {
            writeLog(cfg.logDir, cfg.processName, "Error connecting to DB: " + qscdDB.lastError().text());
        }
    }
}

void MainClass::readStationListfromDB()
{
    staticStaList.clear();
    prexStaList.clear();

    QString query;
    query = "SELECT * FROM NETWORK";
    openDB();
    networkModel->setQuery(query);

    if(networkModel->rowCount() > 0)
    {
        for(int i=0;i<networkModel->rowCount();i++)
        {
            QString network = networkModel->record(i).value("net").toString();

            if(network.startsWith("KG") || network.startsWith("KS"))
            {
                query = "SELECT * FROM AFFILIATION where net='" + network + "'";
                affiliationModel->setQuery(query);

                for(int j=0;j<affiliationModel->rowCount();j++)
                {
                    QString affiliation = affiliationModel->record(j).value("aff").toString();
                    QString affiliationName = affiliationModel->record(j).value("affname").toString();
                    float lat = affiliationModel->record(j).value("lat").toDouble();
                    float lon = affiliationModel->record(j).value("lon").toDouble();
                    query = "SELECT * FROM SITE where aff='" + affiliation + "'";
                    siteModel->setQuery(query);

                    for(int k=0;k<siteModel->rowCount();k++)
                    {
                        _STATION sta;
                        QString staS = siteModel->record(k).value("sta").toString();
                        strcpy(sta.netSta, staS.toLatin1().constData());
                        sta.lat = lat;
                        sta.lon = lon;
                        ll2xy4Small(pj_longlat, pj_eqc, sta.lon, sta.lat, &sta.smapX, &sta.smapY);
                        ll2xy4Large(pj_longlat, pj_eqc, sta.lon, sta.lat, &sta.lmapX, &sta.lmapY);
                        sta.inUse = siteModel->record(k).value("inuse").toInt();
                        if(sta.inUse != 1)
                            continue;
                        sta.pga[0] = 0;sta.pga[1] = 0;sta.pga[2] = 0;sta.pga[3] = 0;sta.pga[4] = 0;
                        sta.pgaTime = 0;

                        staticStaList.append(sta);
                        prexStaList.append(sta);
                    }
                }
            }
        }
    }

    writeLog(cfg.logDir, cfg.processName, "Succedd Reading Station List from Database");
    writeLog(cfg.logDir, cfg.processName, "The number of the Stations : " + QString::number(staticStaList.size()));
}

void MainClass::initProj()
{
    if (!(pj_longlat = pj_init_plus("+proj=longlat +ellps=WGS84")) )
    {
        qDebug() << "Can't initialize projection.";
        exit(1);
    }

    if (!(pj_eqc = pj_init_plus("+proj=eqc +ellps=WGS84")) )
    {
        qDebug() << "Can't initialize projection.";
        exit(1);
    }
}



ProcessQSCDThread::ProcessQSCDThread(QWebSocket *socket, QWidget *parent)
{
    pSocket = socket;
}

ProcessQSCDThread::~ProcessQSCDThread()
{
}

void ProcessQSCDThread::genData(QString message)
{
    if(message.startsWith("Hello"))
        return;

    int chanID = message.section("_", 0, 0).toInt();
    int dataTime = message.section("_", 1, 1).toInt();
    int type = message.section("_", 2, 2).toInt();

    QByteArray data = generateData(chanID, dataTime, type);
    sendBinaryMessage(data);
}

void ProcessQSCDThread::fillPGAintoStaList_Parallel(QList<_QSCD_FOR_MULTIMAP> pgaList, int dataEpochTime)
{
    prexPgaList = pgaList;
    prexDataEpochTime = dataEpochTime;

    std::function<void(_STATION&)> fillPGAintoStaList = [](_STATION &station)
    {
        for(int i=0;i<prexPgaList.size();i++)
        {
            _QSCD_FOR_MULTIMAP qfmm = prexPgaList.at(i);

            if(QString(station.netSta).startsWith(qfmm.netSta))
            {
                for(int j=0;j<5;j++)
                    station.pga[j] = qfmm.pga[j];
                station.pgaTime = prexDataEpochTime;
                break;
            }
        }
    };

    QFuture<void> future = QtConcurrent::map(staticStaList, fillPGAintoStaList);
    future.waitForFinished();
}

QByteArray ProcessQSCDThread::generateData(int chanID, int dt, int type)
{
    QList<_QSCD_FOR_MULTIMAP> pgaList = QSCD_DATA_HOUSE.values(dt);
    QByteArray data;

    monChanID = chanID;

    QList<_STATION> realStaList;
    fillPGAintoStaList_Parallel(pgaList, dt);

    for(int i=0;i<staticStaList.size();i++)
    {
        _STATION station = staticStaList.at(i);
        if(station.pgaTime == dt)
        {
            realStaList.append(station);
        }
    }

    // get IDW again, because the real number of station is diferrent with prexStaList's size
    if(realStaList.size() != prexStaList.size())
    {
        prexStaList.clear();
        prexStaList = realStaList;
        initIDW();

        if(type == 0)
        {
            _BINARY_POINT_PACKET mypacket;
            mypacket.dataTime = 0;
            QDataStream stream(&data, QIODevice::WriteOnly);
            stream.writeRawData((char*)&mypacket, sizeof(_BINARY_POINT_PACKET));

            return data;
        }
        else if(type == 1)
        {
            _BINARY_PGA_PACKET mypacket;
            mypacket.dataTime = 0;
            mypacket.numStation = 0;
            QDataStream stream(&data, QIODevice::WriteOnly);
            stream.writeRawData((char*)&mypacket, sizeof(_BINARY_PGA_PACKET));

            return data;
        }
    }

    tempStaList = realStaList;

    if(type == 0)
    {
        /*
        for(int i=0;i<LANDXYCNT_VERSION_1;i++)
        {
            _POINT mypoint = points.at(i);
            mypoint.staList = realStaList;
            points.replace(i, mypoint);
        }
        */

        QFuture<void> future = QtConcurrent::map(points, getMapZValue);
        future.waitForFinished();

        _BINARY_POINT_PACKET mypacket;

        mypacket.dataTime = dt;
        for(int i=0;i<LANDXYCNT_VERSION_1;i++)
            mypacket.mapZ[i] = points.at(i).mapZ;

        QDataStream stream(&data, QIODevice::WriteOnly);
        stream.writeRawData((char*)&mypacket, sizeof(_BINARY_POINT_PACKET));
    }
    else if(type == 1)
    {
        _BINARY_PGA_PACKET mypacket;

        mypacket.dataTime = dt;
        mypacket.numStation = realStaList.size();

        for(int i=0;i<realStaList.size();i++)
        {
            _STATION station = realStaList.at(i);
            mypacket.staList[i] = station;
        }

        QDataStream stream(&data, QIODevice::WriteOnly);
        stream.writeRawData((char*)&mypacket, sizeof(_BINARY_PGA_PACKET));
    }

    return data;
}

void ProcessQSCDThread::sendBinaryMessage(QByteArray data)
{
    pSocket->sendBinaryMessage(data);
}
