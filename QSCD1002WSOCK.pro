QT -= gui

QT += sql concurrent websockets

CONFIG += c++11 console
CONFIG -= app_bundle

# You can make your code fail to compile if it uses deprecated APIs.
# In order to do so, uncomment the following line.
#DEFINES += QT_DISABLE_DEPRECATED_BEFORE=0x060000    # disables all the APIs deprecated before Qt 6.0.0

HEADERS += \
    mainclass.h

SOURCES += \
        main.cpp \
        mainclass.cpp

# Default rules for deployment.
qnx: target.path = /tmp/$${TARGET}/bin
else: unix:!android: target.path = /opt/$${TARGET}/bin
!isEmpty(target.path): INSTALLS += target

QMAKE_CFLAGS += "-DACCEPT_USE_OF_DEPRECATED_PROJ_API_H"
QMAKE_CXXFLAGS += "-DACCEPT_USE_OF_DEPRECATED_PROJ_API_H"

LIBS += -lproj -lactivemq-cpp -L/data/4_GIT/QTLIBS/KGEEWLIBS -lKGEEWLIBS
INCLUDEPATH += /usr/local/include/activemq-cpp-3.9.5/
INCLUDEPATH += /usr/local/apr/include/apr-1/
INCLUDEPATH += /data/4_GIT/QTLIBS/KGEEWLIBS/
