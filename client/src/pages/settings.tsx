import { useState, useEffect } from 'react';
import { useQuery, useMutation } from '@tanstack/react-query';
import { queryClient, apiRequest } from '@/lib/queryClient';
import { Button } from '@/components/ui/button';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import { Badge } from '@/components/ui/badge';
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs';
import { Settings, Calendar, CheckCircle, AlertCircle, ExternalLink, Users, Mail, RefreshCw, MessageSquare, Save, Globe } from 'lucide-react';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import { Textarea } from '@/components/ui/textarea';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select';
import { Link } from 'wouter';
import { useToast } from '@/hooks/use-toast';
import { useAuth } from '@/hooks/useAuth';
import TeamManagement from '@/components/team-management';
import { PageNav } from '@/components/page-nav';
import { COUNTRIES, LANGUAGES, getCountryConfig } from '@shared/countries';

export default function SettingsPage() {
  const { user } = useAuth();
  const { toast } = useToast();
  const [isTeamManagementOpen, setIsTeamManagementOpen] = useState(false);
  
  // Google Calendar state
  const [calendarAuthorized, setCalendarAuthorized] = useState(false);
  const [calendarLoading, setCalendarLoading] = useState(true);
  
  // Gmail state
  const [gmailConnected, setGmailConnected] = useState(false);
  
  // Secondary Gmail state
  const [secondaryGmailConnected, setSecondaryGmailConnected] = useState(false);
  const [secondaryGmailEmail, setSecondaryGmailEmail] = useState("");
  
  // SMS Templates state
  const [installMessageTemplate, setInstallMessageTemplate] = useState("");
  const [serviceMessageTemplate, setServiceMessageTemplate] = useState("");
  
  // Region state
  const [selectedCountry, setSelectedCountry] = useState("AU");
  const [selectedLanguage, setSelectedLanguage] = useState("en");
  
  // Invoice search settings
  const [invoiceSearchSubject, setInvoiceSearchSubject] = useState("");
  const [invoiceSearchRecipient, setInvoiceSearchRecipient] = useState("");
  
  // Fetch current user for workspace
  const { data: currentUser } = useQuery<{ id: string; workspaceId: number }>({
    queryKey: ["/api/auth/user"],
  });
  
  const workspaceId = currentUser?.workspaceId;
  
  // Fetch invoice settings (which includes SMS templates)
  const { data: invoiceSettings } = useQuery<any>({
    queryKey: ["/api/workspaces", workspaceId, "invoice-settings"],
    enabled: !!workspaceId,
  });
  
  useEffect(() => {
    if (invoiceSettings) {
      setInstallMessageTemplate(invoiceSettings.installMessageTemplate || "");
      setServiceMessageTemplate(invoiceSettings.serviceMessageTemplate || "");
      setSelectedCountry(invoiceSettings.country || "AU");
      setSelectedLanguage(invoiceSettings.language || "en");
      setInvoiceSearchSubject(invoiceSettings.invoiceSearchSubject || "");
      setInvoiceSearchRecipient(invoiceSettings.invoiceSearchRecipient || "");
    }
  }, [invoiceSettings]);
  
  // Save SMS templates mutation
  const saveSmsTemplatesMutation = useMutation({
    mutationFn: async (data: { installMessageTemplate: string; serviceMessageTemplate: string }) => {
      return await apiRequest("PUT", `/api/workspaces/${workspaceId}/invoice-settings`, data);
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["/api/workspaces", workspaceId, "invoice-settings"] });
      toast({
        title: "SMS Templates Saved",
        description: "Your message templates have been updated.",
      });
    },
    onError: () => {
      toast({
        variant: "destructive",
        title: "Error",
        description: "Failed to save templates. Please try again.",
      });
    },
  });

  const saveInvoiceSearchMutation = useMutation({
    mutationFn: async (data: { invoiceSearchSubject: string; invoiceSearchRecipient: string }) => {
      return await apiRequest("PUT", `/api/workspaces/${workspaceId}/invoice-settings`, data);
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["/api/workspaces", workspaceId, "invoice-settings"] });
      toast({
        title: "Invoice Search Settings Saved",
        description: "Gmail will use your custom search criteria on the next sync.",
      });
    },
    onError: () => {
      toast({
        variant: "destructive",
        title: "Error",
        description: "Failed to save invoice search settings. Please try again.",
      });
    },
  });

  const saveRegionMutation = useMutation({
    mutationFn: async (data: { country: string; language: string }) => {
      return await apiRequest("PUT", `/api/workspaces/${workspaceId}/invoice-settings`, data);
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["/api/workspaces", workspaceId, "invoice-settings"] });
      toast({
        title: "Region Settings Saved",
        description: "Your country and language preferences have been updated.",
      });
    },
    onError: () => {
      toast({
        variant: "destructive",
        title: "Error",
        description: "Failed to save region settings. Please try again.",
      });
    },
  });

  // Fetch Google connection status
  const { data: googleStatus } = useQuery<{ calendarConnected?: boolean; gmailConnected?: boolean }>({
    queryKey: ['/api/google/status'],
    refetchInterval: 5000,
  });

  const { data: secondaryGmailStatus } = useQuery<{ connected: boolean; emailAddress?: string }>({
    queryKey: ['/api/gmail/secondary-status'],
    refetchInterval: 5000,
  });

  useEffect(() => {
    if (secondaryGmailStatus) {
      setSecondaryGmailConnected(secondaryGmailStatus.connected || false);
      setSecondaryGmailEmail(secondaryGmailStatus.emailAddress || "");
    }
  }, [secondaryGmailStatus]);

  useEffect(() => {
    if (googleStatus) {
      setCalendarAuthorized(googleStatus.calendarConnected || false);
      setGmailConnected(googleStatus.gmailConnected || false);
      setCalendarLoading(false);
    }
  }, [googleStatus]);

  const connectSecondaryGmail = async () => {
    try {
      const response = await fetch('/api/gmail/secondary-auth-url', {
        headers: { 'X-Origin': window.location.origin }
      });
      const data = await response.json();
      window.open(data.authUrl, '_blank');
      toast({ title: "Gmail Authorization", description: "Complete the sign-in in the new tab, then return here." });
    } catch (error) {
      toast({ title: "Error", description: "Failed to start Gmail connection", variant: "destructive" });
    }
  };

  const disconnectSecondaryGmail = async () => {
    try {
      await fetch('/api/gmail/secondary-disconnect', { method: 'POST' });
      setSecondaryGmailConnected(false);
      setSecondaryGmailEmail("");
      queryClient.invalidateQueries({ queryKey: ['/api/gmail/secondary-status'] });
      toast({ title: "Disconnected", description: "Secondary Gmail account removed." });
    } catch (error) {
      toast({ title: "Error", description: "Failed to disconnect", variant: "destructive" });
    }
  };

  const connectGoogleCalendar = async () => {
    try {
      const response = await fetch('/api/calendar/auth-url', {
        headers: {
          'X-Origin': window.location.origin
        }
      });
      const data = await response.json();
      
      window.open(data.authUrl, '_blank');
      
      const pollInterval = setInterval(async () => {
        const authCheck = await fetch('/api/calendar/auth-status');
        const authData = await authCheck.json();
        if (authData.authorized) {
          setCalendarAuthorized(true);
          clearInterval(pollInterval);
          toast({
            title: "Google Calendar connected successfully!",
            description: "New appointments will now automatically sync to your calendar."
          });
        }
      }, 2000);
      
      setTimeout(() => clearInterval(pollInterval), 300000);
      
    } catch (error) {
      console.error('Error connecting to Google Calendar:', error);
      toast({
        title: "Error connecting to Google Calendar",
        variant: "destructive"
      });
    }
  };

  const disconnectGoogleCalendar = async () => {
    try {
      const response = await fetch('/api/calendar/disconnect', { method: 'POST' });
      if (response.ok) {
        setCalendarAuthorized(false);
        toast({
          title: "Google Calendar disconnected",
          description: "Calendar sync has been disabled."
        });
      }
    } catch (error) {
      console.error('Error disconnecting calendar:', error);
      toast({
        title: "Error disconnecting calendar",
        variant: "destructive"
      });
    }
  };

  // Sync deleted calendar events
  const syncCalendarMutation = useMutation({
    mutationFn: async () => {
      const response = await apiRequest('POST', '/api/calendar/sync-deleted-events', {});
      return response.json();
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['/api/jobs'] });
      toast({ 
        title: "Calendar synced successfully", 
        description: "Checked for deleted events and updated job statuses" 
      });
    },
    onError: () => {
      toast({ title: "Failed to sync calendar", variant: "destructive" });
    },
  });

  // Fix calendar event times
  const fixCalendarTimesMutation = useMutation({
    mutationFn: async () => {
      const response = await apiRequest('POST', '/api/calendar/fix-existing-times', {});
      return response.json();
    },
    onSuccess: (data: any) => {
      queryClient.invalidateQueries({ queryKey: ['/api/jobs'] });
      toast({ 
        title: "Calendar times fixed", 
        description: `Updated ${data?.fixedCount || 0} calendar events to match database times. If you don't see changes, refresh your Google Calendar.`
      });
    },
    onError: () => {
      toast({ title: "Failed to fix calendar times", variant: "destructive" });
    },
  });

  return (
    <div className="min-h-screen bg-gray-50 dark:bg-gray-900">
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
        <header className="border-b border-gray-200 dark:border-gray-700 pb-4">
          <div className="flex items-center justify-between py-4">
            <div className="flex items-center space-x-2">
              <div className="flex items-center">
                <span className="text-2xl font-bold text-blue-600 dark:text-blue-400">FITTER PRO</span>
              </div>
            </div>
            
            <div className="hidden md:flex items-center space-x-4">
              <span className="text-sm text-gray-500 dark:text-white">
                Welcome back, {(user as any)?.firstName || (user as any)?.email || 'User'}
              </span>
              {((user as any)?.role === 'owner' || (user as any)?.role === 'admin') && (
                <Button 
                  onClick={() => setIsTeamManagementOpen(true)}
                  variant="outline" 
                  size="sm" 
                  className="border-purple-600 text-purple-600 hover:bg-purple-50"
                >
                  <Users className="w-4 h-4 mr-2" />
                  Team
                </Button>
              )}
              <Link href="/settings">
                <Button 
                  variant="default"
                  size="sm"
                  className="bg-gray-600 hover:bg-gray-700"
                >
                  <Settings className="w-4 h-4 mr-2" />
                  Settings
                </Button>
              </Link>
              <Button 
                onClick={() => window.location.href = '/api/logout'}
                variant="outline" 
                size="sm"
                className="border-gray-600 text-gray-600 hover:bg-gray-50"
              >
                Sign Out
              </Button>
              
            </div>
            
            <div className="md:hidden flex items-center space-x-2">
              {((user as any)?.role === 'owner' || (user as any)?.role === 'admin') && (
                <Button 
                  onClick={() => setIsTeamManagementOpen(true)}
                  variant="outline" 
                  size="sm" 
                  className="border-purple-600 text-purple-600 hover:bg-purple-50 p-2"
                >
                  <Users className="w-4 h-4" />
                </Button>
              )}
              <Link href="/settings">
                <Button 
                  variant="default"
                  size="sm"
                  className="bg-gray-600 hover:bg-gray-700 p-2"
                >
                  <Settings className="w-4 h-4" />
                </Button>
              </Link>
              <Button 
                onClick={() => window.location.href = '/api/logout'}
                variant="outline" 
                size="sm"
                className="border-gray-600 text-gray-600 hover:bg-gray-50 text-xs px-2"
              >
                Out
              </Button>
              
            </div>
          </div>

          <PageNav currentPage="settings" />
        </header>

        <main className="py-8">
          <div className="mb-6">
            <h1 className="text-2xl font-bold text-gray-900 dark:text-white flex items-center gap-2">
              <Settings className="w-6 h-6" />
              Settings
            </h1>
            <p className="text-gray-600 dark:text-gray-400 mt-1">
              Manage your integrations and preferences
            </p>
          </div>

          <Tabs defaultValue="google-calendar" className="space-y-6">
            <TabsList className="grid w-full max-w-lg grid-cols-4">
              <TabsTrigger value="google-calendar" className="flex items-center gap-2">
                <Calendar className="w-4 h-4" />
                Calendar
              </TabsTrigger>
              <TabsTrigger value="email" className="flex items-center gap-2">
                <Mail className="w-4 h-4" />
                Email
              </TabsTrigger>
              <TabsTrigger value="sms-templates" className="flex items-center gap-2">
                <MessageSquare className="w-4 h-4" />
                SMS
              </TabsTrigger>
              <TabsTrigger value="region" className="flex items-center gap-2">
                <Globe className="w-4 h-4" />
                Region
              </TabsTrigger>
            </TabsList>

            <TabsContent value="google-calendar">
              <Card>
                <CardHeader>
                  <CardTitle className="flex items-center gap-2">
                    <Calendar className="w-5 h-5" />
                    Google Calendar Integration
                  </CardTitle>
                  <CardDescription>
                    Connect your Google Calendar to automatically sync job appointments
                  </CardDescription>
                </CardHeader>
                <CardContent className="space-y-6">
                  <div className="flex items-center justify-between p-4 bg-gray-50 dark:bg-gray-800 rounded-lg">
                    <div className="flex items-center gap-3">
                      <div className={`w-10 h-10 rounded-full flex items-center justify-center ${
                        calendarAuthorized ? 'bg-green-100 dark:bg-green-900/30' : 'bg-gray-200 dark:bg-gray-700'
                      }`}>
                        {calendarAuthorized ? (
                          <CheckCircle className="w-5 h-5 text-green-600" />
                        ) : (
                          <AlertCircle className="w-5 h-5 text-gray-400" />
                        )}
                      </div>
                      <div>
                        <p className="font-medium text-gray-900 dark:text-white">
                          {calendarAuthorized ? 'Connected' : 'Not Connected'}
                        </p>
                        <p className="text-sm text-gray-500">
                          {calendarAuthorized 
                            ? 'Your appointments sync automatically' 
                            : 'Connect to sync job appointments'}
                        </p>
                      </div>
                    </div>
                    <Badge variant={calendarAuthorized ? 'default' : 'secondary'}>
                      {calendarAuthorized ? 'Active' : 'Inactive'}
                    </Badge>
                  </div>

                  {calendarLoading ? (
                    <div className="text-center py-4">
                      <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-blue-600 mx-auto"></div>
                      <p className="text-sm text-gray-500 mt-2">Checking connection status...</p>
                    </div>
                  ) : calendarAuthorized ? (
                    <div className="space-y-4">
                      <div className="p-4 bg-green-50 dark:bg-green-900/20 border border-green-200 dark:border-green-800 rounded-lg">
                        <h4 className="font-medium text-green-800 dark:text-green-200 mb-2">
                          What's synced automatically:
                        </h4>
                        <ul className="text-sm text-green-700 dark:text-green-300 space-y-1">
                          <li>• Job appointments when bookings are confirmed</li>
                          <li>• Time changes when jobs are rescheduled</li>
                          <li>• Job details in calendar event descriptions</li>
                        </ul>
                      </div>

                      {/* Calendar Maintenance Tools */}
                      <div className="p-4 bg-gray-50 dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-lg">
                        <h4 className="font-medium text-gray-800 dark:text-gray-200 mb-3">
                          Calendar Maintenance
                        </h4>
                        <div className="flex flex-wrap gap-3">
                          <Button 
                            onClick={() => syncCalendarMutation.mutate()}
                            disabled={syncCalendarMutation.isPending}
                            variant="outline"
                            className="border-blue-500 text-blue-600 hover:bg-blue-50"
                          >
                            <RefreshCw className={`w-4 h-4 mr-2 ${syncCalendarMutation.isPending ? 'animate-spin' : ''}`} />
                            {syncCalendarMutation.isPending ? 'Syncing...' : 'Sync Calendar'}
                          </Button>
                          <Button 
                            onClick={() => fixCalendarTimesMutation.mutate()}
                            disabled={fixCalendarTimesMutation.isPending}
                            variant="outline"
                            className="border-orange-500 text-orange-600 hover:bg-orange-50"
                          >
                            <RefreshCw className={`w-4 h-4 mr-2 ${fixCalendarTimesMutation.isPending ? 'animate-spin' : ''}`} />
                            {fixCalendarTimesMutation.isPending ? 'Fixing...' : 'Fix Times'}
                          </Button>
                        </div>
                        <p className="text-xs text-gray-500 dark:text-gray-400 mt-2">
                          Sync checks for deleted events. Fix Times updates calendar events to match database times.
                        </p>
                      </div>
                      
                      <Button 
                        onClick={disconnectGoogleCalendar}
                        variant="outline"
                        className="border-red-600 text-red-600 hover:bg-red-50"
                      >
                        Disconnect Calendar
                      </Button>
                    </div>
                  ) : (
                    <div className="space-y-4">
                      <div className="p-4 bg-blue-50 dark:bg-blue-900/20 border border-blue-200 dark:border-blue-800 rounded-lg">
                        <h4 className="font-medium text-blue-800 dark:text-blue-200 mb-2">
                          Benefits of connecting:
                        </h4>
                        <ul className="text-sm text-blue-700 dark:text-blue-300 space-y-1">
                          <li>• Automatically create calendar events for booked jobs</li>
                          <li>• Keep your schedule synced across all devices</li>
                          <li>• Get reminders for upcoming appointments</li>
                        </ul>
                      </div>
                      
                      <Button 
                        onClick={connectGoogleCalendar}
                        className="bg-blue-600 hover:bg-blue-700"
                      >
                        <ExternalLink className="w-4 h-4 mr-2" />
                        Connect Google Calendar
                      </Button>
                    </div>
                  )}
                </CardContent>
              </Card>
            </TabsContent>

            <TabsContent value="email">
              <Card>
                <CardHeader>
                  <CardTitle className="flex items-center gap-2">
                    <Mail className="w-5 h-5" />
                    Email Sync Settings
                  </CardTitle>
                  <CardDescription>
                    Manage Gmail integration for importing job sheets and invoices
                  </CardDescription>
                </CardHeader>
                <CardContent className="space-y-6">
                  <div className="flex items-center justify-between p-4 bg-gray-50 dark:bg-gray-800 rounded-lg">
                    <div className="flex items-center gap-3">
                      <div className={`w-10 h-10 rounded-full flex items-center justify-center ${
                        gmailConnected ? 'bg-green-100 dark:bg-green-900/30' : 'bg-gray-200 dark:bg-gray-700'
                      }`}>
                        {gmailConnected ? (
                          <CheckCircle className="w-5 h-5 text-green-600" />
                        ) : (
                          <AlertCircle className="w-5 h-5 text-gray-400" />
                        )}
                      </div>
                      <div>
                        <p className="font-medium text-gray-900 dark:text-white">
                          {gmailConnected ? 'Gmail Connected' : 'Gmail Not Connected'}
                        </p>
                        <p className="text-sm text-gray-500">
                          {gmailConnected 
                            ? 'Job sheets are imported automatically' 
                            : 'Connect Gmail to auto-import job sheets'}
                        </p>
                      </div>
                    </div>
                    <Link href="/email-integration">
                      <Button variant="outline">
                        Manage Email Settings
                      </Button>
                    </Link>
                  </div>

                  <div className="border-t pt-6">
                    <h3 className="text-base font-semibold mb-1">Secondary Gmail Account (for Invoices)</h3>
                    <p className="text-sm text-muted-foreground mb-4">
                      Connect a second Gmail account to scan for invoices. Useful if your invoices go to a different email than your primary login.
                    </p>
                    <div className="flex items-center justify-between p-4 bg-gray-50 dark:bg-gray-800 rounded-lg">
                      <div className="flex items-center gap-3">
                        <div className={`w-10 h-10 rounded-full flex items-center justify-center ${
                          secondaryGmailConnected ? 'bg-green-100 dark:bg-green-900/30' : 'bg-gray-200 dark:bg-gray-700'
                        }`}>
                          {secondaryGmailConnected ? (
                            <CheckCircle className="w-5 h-5 text-green-600" />
                          ) : (
                            <AlertCircle className="w-5 h-5 text-gray-400" />
                          )}
                        </div>
                        <div>
                          <p className="font-medium text-gray-900 dark:text-white">
                            {secondaryGmailConnected ? `Connected: ${secondaryGmailEmail}` : 'No secondary account'}
                          </p>
                          <p className="text-sm text-gray-500">
                            {secondaryGmailConnected 
                              ? 'Invoice emails will be scanned from this account too' 
                              : 'Add a second Gmail to scan for invoice emails'}
                          </p>
                        </div>
                      </div>
                      {secondaryGmailConnected ? (
                        <Button variant="outline" onClick={disconnectSecondaryGmail}>
                          Disconnect
                        </Button>
                      ) : (
                        <Button onClick={connectSecondaryGmail}>
                          Connect Gmail
                        </Button>
                      )}
                    </div>
                  </div>

                  <div className="border-t pt-6">
                    <h3 className="text-base font-semibold mb-1">Invoice Email Search</h3>
                    <p className="text-sm text-muted-foreground mb-4">
                      Configure how Gmail searches for your invoices in Earnings. Enter the email subject line and recipient to match your invoicing app's emails.
                    </p>
                    <div className="space-y-4">
                      <div>
                        <Label htmlFor="invoice-search-subject">Invoice Email Subject</Label>
                        <Input
                          id="invoice-search-subject"
                          placeholder='e.g. "Invoice from Emerson Redondo"'
                          value={invoiceSearchSubject}
                          onChange={(e) => setInvoiceSearchSubject(e.target.value)}
                          className="mt-1"
                        />
                        <p className="text-xs text-muted-foreground mt-1">
                          The subject line your invoicing app uses when sending invoices
                        </p>
                      </div>
                      <div>
                        <Label htmlFor="invoice-search-recipient">Recipient Email</Label>
                        <Input
                          id="invoice-search-recipient"
                          placeholder="e.g. service@curtainworld.net.au"
                          value={invoiceSearchRecipient}
                          onChange={(e) => setInvoiceSearchRecipient(e.target.value)}
                          className="mt-1"
                        />
                        <p className="text-xs text-muted-foreground mt-1">
                          The email address your invoices are sent to
                        </p>
                      </div>
                      <Button
                        onClick={() => saveInvoiceSearchMutation.mutate({
                          invoiceSearchSubject,
                          invoiceSearchRecipient,
                        })}
                        disabled={saveInvoiceSearchMutation.isPending}
                        className="w-full sm:w-auto"
                      >
                        {saveInvoiceSearchMutation.isPending ? (
                          <>
                            <RefreshCw className="w-4 h-4 mr-2 animate-spin" />
                            Saving...
                          </>
                        ) : (
                          <>
                            <Save className="w-4 h-4 mr-2" />
                            Save Invoice Search Settings
                          </>
                        )}
                      </Button>
                    </div>
                  </div>
                </CardContent>
              </Card>
            </TabsContent>

            <TabsContent value="sms-templates">
              <Card>
                <CardHeader>
                  <CardTitle className="flex items-center gap-2">
                    <MessageSquare className="w-5 h-5" />
                    SMS Message Templates
                  </CardTitle>
                  <CardDescription>
                    Customize SMS message templates for different job types. Available placeholders: {"{clientName}"} (client's name), {"{userName}"} (your name), {"{jobId}"} (shortened job number like "8134-1"), {"{date}"} (formatted date), {"{time}"} (time), {"{availability}"} (availability text with date)
                  </CardDescription>
                </CardHeader>
                <CardContent className="space-y-6">
                  <div>
                    <Label htmlFor="install-message">Installation Message Template</Label>
                    <Textarea
                      id="install-message"
                      placeholder="Hi {clientName}, this is {userName} from Curtain World. Your order ({jobId}) for plantation shutters is now ready for installation and my next availability is {availability} at {time}. Please let me know if this is suitable or contact me directly to discuss other arrangements. Kind Regards, {userName} - Curtain World."
                      value={installMessageTemplate}
                      onChange={(e) => setInstallMessageTemplate(e.target.value)}
                      className="min-h-[120px] mt-2"
                    />
                    <p className="text-xs text-muted-foreground mt-1">
                      Used for Install jobs
                    </p>
                  </div>
                  <div>
                    <Label htmlFor="service-message">Service Message Template</Label>
                    <Textarea
                      id="service-message"
                      placeholder="Hi {clientName}, this is {userName} from Curtain World. Your service order ({jobId}) for plantation shutters is now ready and my next availability is on {date} at {time}. Please let me know if this is suitable or contact me directly to discuss other arrangements. Kind Regards, {userName} - Curtain World."
                      value={serviceMessageTemplate}
                      onChange={(e) => setServiceMessageTemplate(e.target.value)}
                      className="min-h-[120px] mt-2"
                    />
                    <p className="text-xs text-muted-foreground mt-1">
                      Used for Service jobs
                    </p>
                  </div>
                  <div className="flex justify-end">
                    <Button
                      onClick={() => saveSmsTemplatesMutation.mutate({ installMessageTemplate, serviceMessageTemplate })}
                      disabled={saveSmsTemplatesMutation.isPending}
                    >
                      <Save className="mr-2 h-4 w-4" />
                      {saveSmsTemplatesMutation.isPending ? "Saving..." : "Save Templates"}
                    </Button>
                  </div>
                </CardContent>
              </Card>
            </TabsContent>

            <TabsContent value="region">
              <Card>
                <CardHeader>
                  <CardTitle className="flex items-center gap-2">
                    <Globe className="w-5 h-5" />
                    Country & Language
                  </CardTitle>
                  <CardDescription>
                    Set your country to adjust financial year dates, currency formatting, and tax labels across the app
                  </CardDescription>
                </CardHeader>
                <CardContent className="space-y-6">
                  <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
                    <div className="space-y-2">
                      <Label>Country</Label>
                      <Select value={selectedCountry} onValueChange={setSelectedCountry}>
                        <SelectTrigger>
                          <SelectValue placeholder="Select country" />
                        </SelectTrigger>
                        <SelectContent>
                          {COUNTRIES.map(c => (
                            <SelectItem key={c.code} value={c.code}>
                              {c.flag} {c.name}
                            </SelectItem>
                          ))}
                        </SelectContent>
                      </Select>
                      {selectedCountry && (() => {
                        const config = getCountryConfig(selectedCountry);
                        const monthNames = ["January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"];
                        return (
                          <div className="mt-3 p-3 bg-muted rounded-lg space-y-1">
                            <p className="text-sm"><span className="font-medium">Currency:</span> {config.currency} ({config.currencySymbol})</p>
                            <p className="text-sm"><span className="font-medium">Financial Year Starts:</span> {monthNames[config.fyStartMonth]}</p>
                            <p className="text-sm"><span className="font-medium">Tax ID Label:</span> {config.taxLabel}</p>
                            <p className="text-sm"><span className="font-medium">Date Format:</span> {config.dateFormat}</p>
                          </div>
                        );
                      })()}
                    </div>
                    <div className="space-y-2">
                      <Label>Language</Label>
                      <Select value={selectedLanguage} onValueChange={setSelectedLanguage}>
                        <SelectTrigger>
                          <SelectValue placeholder="Select language" />
                        </SelectTrigger>
                        <SelectContent>
                          {LANGUAGES.map(l => (
                            <SelectItem key={l.code} value={l.code}>
                              {l.flag} {l.name}
                            </SelectItem>
                          ))}
                        </SelectContent>
                      </Select>
                      <p className="text-xs text-muted-foreground">
                        Language preference for future translations
                      </p>
                    </div>
                  </div>
                  <div className="flex justify-end">
                    <Button
                      onClick={() => saveRegionMutation.mutate({ country: selectedCountry, language: selectedLanguage })}
                      disabled={saveRegionMutation.isPending}
                    >
                      <Save className="mr-2 h-4 w-4" />
                      {saveRegionMutation.isPending ? "Saving..." : "Save Region Settings"}
                    </Button>
                  </div>
                </CardContent>
              </Card>
            </TabsContent>
          </Tabs>
        </main>
      </div>

      <TeamManagement 
        isOpen={isTeamManagementOpen}
        onClose={() => setIsTeamManagementOpen(false)}
      />
    </div>
  );
}
