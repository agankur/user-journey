#!/usr/local/bin/perl
use strict;
use Cwd;
use Getopt::Long;
#############################################################################

my $gEnv = $ENV{'env'};
my $gSilentFlag;
my $gVersion;
my $gInstallDir;
my $gRepoDir;
my $gForceDeploy;
my $gModule;
##############################################################################


##############################################################################
#
# Sub verifyBuild()
#
##############################################################################
sub verifyBuild()
{
  my $repoDir = "$gRepoDir".'/'."deploy";

  # Build the project
  {
    logMessage("INFO:Building the project");
    my $curDir = getcwd;
    chdir "$gRepoDir/sources/scala";
    system("export env=$gEnv ; /usr/local/bin/sbt package");
    chdir $gRepoDir;
    system("export env=$gEnv ; ant clean ; ant");
    chdir $curDir;
  }

  if (! -d $gRepoDir)
  {
    logMessage("ERROR: Repo dir - $repoDir does not exist. No source to deploy");
    logMessage("ERROR: Please provide the correct repository dir : Script aborted");
    exit;
  }
}
##############################################################################


##############################################################################
#
# Sub verifyENV()
#
##############################################################################
sub verifyENV()
{
  if (! $gEnv)
   {
     logMessage("ERROR: env not set");
     exit;
   }
  if (($gEnv ne "prod") && ($gEnv ne "dev"))
   {
     logMessage("ERROR: env - $gEnv not correct : Should be either dev or prod");
     exit;
   }

  if (! $gInstallDir)
   {
     $gInstallDir = "/opt/bsb/$gModule";
     logMessage("WARN: Base Path not provided using default - $gInstallDir");
   }
  if (! $gRepoDir)
   {
     $gRepoDir = "/mnt/git-repo/user-journey";
     logMessage("WARN: Repo dir not provided using default - $gRepoDir");
   }
}
##############################################################################


##############################################################################
#
# Sub deployVersion()
#
##############################################################################
sub deployVersion()
{
  if (! -d $gInstallDir)
  {
    logMessage("WARN: Install dir - not exist : creating base dir");
    logMessage("WARN: Assuming fresh installation");
    system("mkdir -p $gInstallDir");
  }

  logMessage("INFO: Deploying to  - $gInstallDir/$gVersion");
  system("mkdir -p $gInstallDir/$gVersion") if ( ! -d "$gInstallDir/$gVersion");
  if (-d "$gInstallDir/$gVersion")
  {
    my $cmd = "rsync -a $gRepoDir/deploy/ $gInstallDir/$gVersion/";
    system("$cmd");
  }
  system("cp $gRepoDir/deploy/user-journey.jar $gInstallDir/$gVersion/lib/UserJourney.jar");
  system("rm -rf $gInstallDir/$gVersion/user-journey.jar");

  logMessage("INFO: Updating current");
  unlink "$gInstallDir/current" if (-e "$gInstallDir/current");
  my $curDir = getcwd;
  chdir $gInstallDir;
  system("ln -s $gVersion current");
  chdir $curDir;
  logMessage("INFO: Updating current - done");
}
##############################################################################


##############################################################################
#
# Sub usage()
#
##############################################################################
sub usage()
{
  print "Usage: $0 -v <Version number> -i <Install dir> -r <Repository dir> -h|help\n";
  print "\t -env <env either dev or prod>";
  print "\t -m <Module to be deployed eg. twang or maanalytics or schoopwhoop or npd or sojourn>";
  print "\t -v <Version number>  : This version number \n";
  print "\t -i <Install dir>     : Install dir default is /opt/bsb/sojourn \n";
  print "\t -r <Repository dir>  : Repository / Source dir default is /mnt/git-repo/user-journey \n";
  print "\t -h|help              : help message\n";
  exit();
}
##############################################################################


##############################################################################
#
# Sub Main()
#
##############################################################################
sub Main()
{
  my($result,$help);
  $result = GetOptions ("v=s"       => \$gVersion,
                        "env=s"     => \$gEnv,
                        "i=s"       => \$gInstallDir,
                        "r=s"       => \$gRepoDir,
                        "q"         => \$gSilentFlag,
                        "f"         => \$gForceDeploy,
                        "m=s"       => \$gModule,
                        "h|help"    => \$help);

  usage() if ( ($help) || (! $gVersion) || (! $gEnv) || (! $gModule));
  logMessage("INFO: Verifying environment");
  verifyENV();
  logMessage("INFO: Verifying environment - done");
  logMessage("INFO: Verifying existing version");
  if (($gEnv eq "prod") && (-d "$gInstallDir/$gVersion") && (! $gForceDeploy))
  {
    logMessage("WARN: Version=$gVersion Already deployed");
    logMessage("WARN: Nothing to be done");
    exit;
  }
  logMessage("INFO: Verifying existing version - done");

  logMessage("INFO: Verifying build");
  verifyBuild();
  logMessage("INFO: Verifying build - done");

  logMessage("INFO: deploying version - $gVersion");
  deployVersion();
  logMessage("INFO: deployed version - $gVersion : Successfully");
}
##############################################################################


##############################################################################
#
# Sub logMessage()
#
##############################################################################
sub logMessage()
{
  if (! $gSilentFlag)
  {
    my($msg) = @_;
    my $timeStamp = localtime;
    print "[$timeStamp] [Info] [$msg]\n";
  }
}
##############################################################################


#############################################################################
#
# Script Entry
#
##############################################################################

Main();
##############################################################################
