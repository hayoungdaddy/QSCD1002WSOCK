#include <QCoreApplication>

#include "mainclass.h"

int main(int argc, char *argv[])
{
    QCoreApplication a(argc, argv);

    MainClass m(argv[1]);

    return a.exec();
}
