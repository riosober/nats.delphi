unit NATS.Wrapper;

interface

uses
  System.Classes, System.SysUtils, System.Math, System.Generics.Collections, Winapi.Windows, System.JSON.Serializers,
  NATS.Connection, NATS.Classes, NATS.Entities, StrUtils;

type
  TNATSCommander = record
    seq: integer; // command seq
    reply: string; // command package for recv stable reply subject
    subject: string; // subject to send
    cmd: string; // command name
    data: string; // biz data
    function ToJSONString: string;
    class function FromJSONString(const AValue: string): TNATSCommander; static;
  end;

  TArrayNATSCommander = class(TList<TNATSCommander>)
  end;

  {
    Data package like a Command for user Interface;
  }
  TNATSCommandHandler = reference to procedure(const ACmd: TNATSCommander);

type
  TNATSMessageRecord = record
    id: string;
    tick: integer;
    msg: TNATSCommander;
  end;

  TArrayNATSMessage = class(TList<TNATSMessageRecord>)
  end;

  TNATSSubscribeRecord = record
    id: integer;
    subject: string;
    MsgHandler: TNATSCommandHandler;
  end;

  TArrayNATSSubscribe = class(TList<TNATSSubscribeRecord>)
  end;

type
  TSequenceManager = Class(TObject)
  private
    FCmd_Sequence: integer;
    FRandom_id: String;
  public
    function get_cmd_sequence(): integer;
    function get_subject_reply_me(): string;
    function get_cmd_stable_reply(): string;
    Constructor Create;
  End;

type
  TMQCommunicationWrapper = Class(TObject)
  private
    // NATS ���Ӷ���
    FNATSObject: TNatsConnection;
    FConnected: Boolean;
    // ������������Ϣ
    FHost: String;
    FPort: integer;
    FUser: String;
    FPass: string;
    FTimeOunt: integer;
    // �����Ǵ�����Ϣ
    FServerInfo: TNatsServerInfo;
    procedure ReleaseAll();
    procedure get_new_nats_connector();
  protected
    procedure OnConnect(AInfo: TNatsServerInfo);
    procedure OnDisconnect();
  public
    // ����
    constructor Create(Host: string; Port: integer; const User: String = ''; const Pass: string = '';
      const ATimeout: integer = 1000);
    destructor Destroy; override;
    // ����ӿ�
    function CheckConnect(): Boolean;
    function Subscribe_One(subject: string; AMsgHandler: TNatsMsgHandler): integer;
    procedure UnSubscribe_One(AId: integer; CONST AMaxMsg: Cardinal = 0); overload;
    procedure UnSubscribe_One(const ASubject: string; CONST AMaxMsg: Cardinal = 0); overload;
    procedure Publish_One(subject: string; msg_string: string; const reply: string = '');
    property ServerInfo: TNatsServerInfo read FServerInfo write FServerInfo;
  End;

  TBizMQProcessor = Class(TThread)
  private
    // ͨѶ����
    FNATS: TMQCommunicationWrapper;
    // ���ĺ���Ϣ����
    FSubscribes: TArrayNATSSubscribe;
    FSendMessages: TArrayNATSCommander;
    FRecvMessages: TArrayNATSCommander;
    // �ط���;
    FReSendMessages: TArrayNATSMessage;
    FReRecvMessages: TArrayNATSMessage;
    FError: string;
    procedure DoExecute;
    function findSubscribeBy(subject: string): TNATSSubscribeRecord;
    procedure DeleteResendMsg(msg: TNATSCommander);
    function IsReceivedMsg(msg: TNATSCommander): Boolean;
    function ResendMessage(): Boolean;
    procedure RecordSendStatus(msg: TNATSCommander);
  protected
    procedure OnMessage(const AMsg: TNatsArgsMSG);
    procedure Execute; override;
  public
    constructor Create(Host: string; Port: integer; const User: String = ''; const Pass: string = '';
      const ATimeout: integer = 1000);
    destructor Destroy; override;
    procedure Subscribe(ASubject: string; AMsgHandler: TNATSCommandHandler);
    procedure USubScribe(ASubject: string);
    procedure Publish(const ASubject, ACmd: string; const AData: string = '');
    procedure SendCommand(const ACmd: TNATSCommander);
    property Error: String Read FError write FError;
  End;

  // command generate/restore API
function get_commander(subject: string; cmd: string; data: string): TNATSCommander;
function restore_commander(subject: string; cmd_data: string): TNATSCommander;

var
  SingletonNATSObject: TBizMQProcessor = nil;

implementation

var
  cmd_seq_obj: TSequenceManager = nil;

function get_commander(subject: string; cmd: string; data: string): TNATSCommander; // # ȡ���������
begin
  Result.seq := cmd_seq_obj.get_cmd_sequence();
  Result.subject := subject;
  Result.reply := cmd_seq_obj.get_subject_reply_me();
  Result.cmd := cmd;
  Result.data := data;
end;

// # ���ַ����ָ�Commander
function restore_commander(subject: string; cmd_data: string): TNATSCommander;
begin
  if cmd_data = '' then
    exit;
  Result := TNATSCommander.FromJSONString(cmd_data);
  Result.subject := subject
end;

{ TSequenceManager }

constructor TSequenceManager.Create;
begin
  FCmd_Sequence := 0;
  FRandom_id := IntToStr(GetCurrentProcessId()) + '_' + IntToStr(Random(1000));
end;

function TSequenceManager.get_cmd_sequence: integer;
begin
  inc(self.FCmd_Sequence);
  Result := self.FCmd_Sequence;
end;

function TSequenceManager.get_cmd_stable_reply: string;
begin
  Result := 'stable_resp'
end;

function TSequenceManager.get_subject_reply_me: string;
begin
  Result := 'stable_reply.' + self.FRandom_id
end;

{ TNATSCommander }

class function TNATSCommander.FromJSONString(const AValue: string): TNATSCommander;
var
  LSer: TJsonSerializer;
begin
  LSer := TJsonSerializer.Create;
  try
    Result := LSer.Deserialize<TNATSCommander>(AValue);
  finally
    LSer.Free;
  end;

end;

function TNATSCommander.ToJSONString: string;
var
  LSer: TJsonSerializer;
begin
  LSer := TJsonSerializer.Create;
  try
    Result := LSer.Serialize<TNATSCommander>(self);
  finally
    LSer.Free;
  end;

end;

{ TMQCommunicationWrapper }

function TMQCommunicationWrapper.CheckConnect(): Boolean;
begin
  Result := False;
  if not Assigned(FNATSObject) then
  begin
    get_new_nats_connector();
    Result := True;
  end
  else if FConnected = False then
  begin
    if FNATSObject.Connected = False then
    begin
      FNATSObject.Connect;
      Result := True;
    end
    else
    begin
      FConnected := True;
    end;
  end;
end;

constructor TMQCommunicationWrapper.Create(Host: string; Port: integer; const User: String = '';
  const Pass: string = ''; const ATimeout: integer = 1000);
begin
  inherited Create();
  FConnected := False;
  FHost := Host;
  FPort := Port;
  FUser := User;
  FPass := Pass;
  FTimeOunt := ATimeout;
  CheckConnect();
end;

destructor TMQCommunicationWrapper.Destroy;
begin
  ReleaseAll();
  inherited;
end;

procedure TMQCommunicationWrapper.get_new_nats_connector;
begin
  FNATSObject := TNatsConnection.Create;
  FNATSObject.SetDefaultOptions(FUser, FPass);
  FNATSObject.SetChannel(FHost, FPort, FTimeOunt).Open(self.OnConnect, self.OnDisconnect);
end;

procedure TMQCommunicationWrapper.OnConnect(AInfo: TNatsServerInfo);
begin
  FServerInfo := AInfo;
  FConnected := True;
end;

procedure TMQCommunicationWrapper.OnDisconnect;
begin
  FConnected := False;
end;

procedure TMQCommunicationWrapper.Publish_One(subject, msg_string: string; const reply: string = '');
begin
  FNATSObject.Publish(subject, msg_string, reply);
end;

procedure TMQCommunicationWrapper.ReleaseAll;
begin
  if Assigned(FNATSObject) then
  begin
    try
      FNATSObject.Close();
    finally
      FreeAndNil(FNATSObject);
    end;
  end;
end;

function TMQCommunicationWrapper.Subscribe_One(subject: string; AMsgHandler: TNatsMsgHandler): integer;
begin
  Result := FNATSObject.Subscribe(subject, AMsgHandler)
end;

procedure TMQCommunicationWrapper.UnSubscribe_One(const ASubject: string; CONST AMaxMsg: Cardinal = 0);
begin
  FNATSObject.UnSubscribe(ASubject, AMaxMsg)
end;

procedure TMQCommunicationWrapper.UnSubscribe_One(AId: integer; CONST AMaxMsg: Cardinal = 0);
begin
  FNATSObject.UnSubscribe(AId, AMaxMsg)
end;

{ TBizMQProcessor }

constructor TBizMQProcessor.Create(Host: string; Port: integer; const User: String = ''; const Pass: string = '';
  const ATimeout: integer = 1000);
begin
  inherited Create(True);
  // ���ĺ���Ϣ����
  FSubscribes := TArrayNATSSubscribe.Create();
  FSendMessages := TArrayNATSCommander.Create;
  FRecvMessages := TArrayNATSCommander.Create;
  // �ط�ȥ��
  FReSendMessages := TArrayNATSMessage.Create;
  FReRecvMessages := TArrayNATSMessage.Create;
  FNATS := TMQCommunicationWrapper.Create(Host, Port, User, Pass, ATimeout);
  self.Subscribe(cmd_seq_obj.get_subject_reply_me, nil);
  Resume();
end;

procedure TBizMQProcessor.DeleteResendMsg(msg: TNATSCommander);
var
  i: integer;
  rMsg: TNATSMessageRecord;
begin
  TMonitor.Enter(FReSendMessages);
  try
    for i := FReSendMessages.Count - 1 downto 0 do
    begin
      rMsg := FReSendMessages.Items[i];
      // OutputDebugString(PWideChar('ɾ���ط���' + rMsg.id + '==?' + msg.cmd));
      if rMsg.id = msg.cmd then
      begin
        FReSendMessages.Remove(rMsg);
        OutputDebugString(PWideChar('deleting re-send queue msg��' + rMsg.msg.ToJSONString));
        break;
      end;
    end;
  finally
    TMonitor.exit(FReSendMessages);
  end;

end;

destructor TBizMQProcessor.Destroy;
var
  i: integer;
  sub: TNATSSubscribeRecord;
begin
  TMonitor.Enter(FSubscribes);
  try
    for i := FSubscribes.Count - 1 downto 0 do
    begin
      try
        sub := FSubscribes.Items[i];
        FSubscribes.Remove(sub);
        FNATS.UnSubscribe_One(sub.subject);
      except
        on e: Exception do
          Error := '';
      end;
    end;
  finally
    TMonitor.exit(FSubscribes)
  end;
  FreeAndNil(FNATS);
  FreeAndNil(FSubscribes);
  FreeAndNil(FSendMessages);
  FreeAndNil(FRecvMessages);
  FreeAndNil(FReSendMessages);
  FreeAndNil(FReRecvMessages);
  inherited;
end;

procedure TBizMQProcessor.OnMessage(const AMsg: TNatsArgsMSG);
var
  ACmd: TNATSCommander;
begin
  TMonitor.Enter(FRecvMessages);
  try
    ACmd := restore_commander(AMsg.subject, AMsg.Payload);
    FRecvMessages.Add(ACmd);
    OutputDebugString(PWideChar('Recv Message:' + ACmd.ToJSONString));
  finally
    TMonitor.exit(FRecvMessages);
  end;

end;

procedure TBizMQProcessor.Publish(const ASubject, ACmd: String; const AData: string = '');
var
  BCmd: TNATSCommander;
begin
  BCmd := get_commander(ASubject, ACmd, AData);
  self.SendCommand(BCmd);
end;

procedure TBizMQProcessor.RecordSendStatus(msg: TNATSCommander);
var
  i: integer;
  rMsg: TNATSMessageRecord;
begin
  i := Pos(cmd_seq_obj.get_cmd_stable_reply(), msg.cmd);
  if i = 0 then
  begin
    TMonitor.Enter(FReSendMessages);
    try
      rMsg.id := cmd_seq_obj.get_cmd_stable_reply() + IntToStr(msg.seq);
      rMsg.tick := GetTickCount();
      rMsg.msg := msg;
      FReSendMessages.Add(rMsg);
      OutputDebugString(PWideChar('Record resend command:' + rMsg.msg.ToJSONString + ' ' + rMsg.id))
    finally
      TMonitor.exit(FReSendMessages);
    end;
  end;
end;

function TBizMQProcessor.ResendMessage: Boolean;
var
  i, pass_t: integer;
  rMsg: TNATSMessageRecord;
begin
  TMonitor.Enter(FReSendMessages);
  try
    for i := FReSendMessages.Count - 1 downto 0 do
    begin
      rMsg := FReSendMessages.Items[i];
      FReSendMessages.Remove(rMsg);
      pass_t := GetTickCount() - rMsg.tick;
      // OutputDebugString(PWideChar('�ط���ʱֵ��' + IntToStr(pass_t) + ':' + IntToStr(rMsg.msg.seq) + '>' +
      // IntToStr(cmd_seq_obj.FCmd_Sequence)));
      // ��ʱ����������������
      if (pass_t > 300 * 1000) or (rMsg.msg.seq > cmd_seq_obj.FCmd_Sequence) then
      begin
        OutputDebugString(PWideChar('Delete resend queue data for timeout:��' + rMsg.msg.ToJSONString));
        continue;
      end;
      // 10�뷢һ��
      if pass_t > 10000 then
      begin
        rMsg.tick := GetTickCount();
        FNATS.Publish_One(rMsg.msg.subject, rMsg.msg.ToJSONString);
        OutputDebugString(PWideChar('Resending: ' + rMsg.msg.ToJSONString))
      end;
      FReSendMessages.Add(rMsg);
    end;
  finally
    TMonitor.exit(FReSendMessages);
  end;
end;

procedure TBizMQProcessor.SendCommand(const ACmd: TNATSCommander);
begin
  TMonitor.Enter(FSendMessages);
  try
    FSendMessages.Add(ACmd);
  finally
    TMonitor.exit(FSendMessages)
  end;

end;

procedure TBizMQProcessor.Subscribe(ASubject: string; AMsgHandler: TNATSCommandHandler);
var
  subs: TNATSSubscribeRecord;
begin
  TMonitor.Enter(FSubscribes);
  try
    subs.subject := ASubject;
    subs.MsgHandler := AMsgHandler;
    subs.id := FNATS.Subscribe_One(ASubject, self.OnMessage);
    FSubscribes.Add(subs);
  finally
    TMonitor.exit(FSubscribes)
  end;
end;

procedure TBizMQProcessor.USubScribe(ASubject: string);
var
  i: integer;
  sub: TNATSSubscribeRecord;
begin
  FNATS.UnSubscribe_One(ASubject);
  TMonitor.Enter(FSubscribes);
  try
    for i := FSubscribes.Count - 1 downto 0 do
    begin
      sub := FSubscribes.Items[i];
      if sub.subject = ASubject then
      begin
        FSubscribes.Remove(sub);
      end;
    end;
  finally
    TMonitor.exit(FSubscribes)
  end;
end;

procedure TBizMQProcessor.DoExecute;
var
  i: integer;
  sub: TNATSSubscribeRecord;
  msg: TNATSCommander;
begin
  while not Terminated do
  begin
    try
      // ���������
      if FNATS.CheckConnect() = True then
      begin
        TMonitor.Enter(FSubscribes);
        try
          for i := 0 to FSubscribes.Count - 1 do
          begin
            sub := FSubscribes.Items[i];
            sub.id := FNATS.Subscribe_One(sub.subject, self.OnMessage);
          end;
        finally
          TMonitor.exit(FSubscribes)
        end;
      end;
      // ������Ϣ���մ���ͻر�
      TMonitor.Enter(FRecvMessages);
      try
        for i := FRecvMessages.Count - 1 downto 0 do
        begin
          msg := FRecvMessages.Items[i];
          FRecvMessages.Remove(msg);
          // ����һ���ر�
          if msg.subject = cmd_seq_obj.get_subject_reply_me() then
          begin
            self.DeleteResendMsg(msg)
          end
          else
          begin
            // ���ͻر�
            self.Publish(msg.reply, cmd_seq_obj.get_cmd_stable_reply + IntToStr(msg.seq));
            if self.IsReceivedMsg(msg) = False then
            begin
              // ���ô�����
              sub := self.findSubscribeBy(msg.subject);
              if Assigned(sub.MsgHandler) = True then
              begin
                sub.MsgHandler(msg);
                OutputDebugString(PWideChar('Call MsgHandler():' + msg.ToJSONString))
              end;
            end;
          end;
        end;
      finally
        TMonitor.exit(FRecvMessages)
      end;
      // ������Ϣ����
      TMonitor.Enter(FSendMessages);
      try
        for i := FSendMessages.Count - 1 downto 0 do
        begin
          msg := FSendMessages.Items[i];
          FSendMessages.Remove(msg);
          // ����
          FNATS.Publish_One(msg.subject, msg.ToJSONString());
          OutputDebugString(PWideChar('Send message:' + msg.ToJSONString));
          // ��¼
          self.RecordSendStatus(msg);
        end;
      finally
        TMonitor.exit(FSendMessages);
      end;
      // ������Ϣ�ط�
      self.ResendMessage();
      sleep(300);
      OutputDebugString(PWideChar(format('Queue Count:%d / %d / %d / %d / %d', [FRecvMessages.Count, FReRecvMessages.Count,
        FSendMessages.Count, FReSendMessages.Count, FSubscribes.Count])));
    except
      on e: Exception do
        Error := e.Message;
    end;
  end;
end;

procedure TBizMQProcessor.Execute;
begin
  NameThreadForDebugging('BizMQProcessor');
  try
    DoExecute;
  except
    on e: Exception do
      Error := e.Message;
  end;

end;

function TBizMQProcessor.findSubscribeBy(subject: string): TNATSSubscribeRecord;
var
  i: integer;
  sub: TNATSSubscribeRecord;
begin
  TMonitor.Enter(FSubscribes);
  try
    Result.MsgHandler := Nil;
    for i := FSubscribes.Count - 1 downto 0 do
    begin
      sub := FSubscribes.Items[i];
      OutputDebugString(PWideChar('search SUB��' + sub.subject + '==?' + subject));
      if sub.subject = subject then
      begin
        Result := sub;
        break;
      end;
    end;
  finally
    TMonitor.exit(FSubscribes);
  end;
end;

function TBizMQProcessor.IsReceivedMsg(msg: TNATSCommander): Boolean;
var
  i: integer;
  rMsg: TNATSMessageRecord;
  msg_id: string;
begin
  TMonitor.Enter(FReRecvMessages);
  try
    Result := False;
    msg_id := msg.reply + '_' + IntToStr(msg.seq);
    // ���Ѿ������˵ļ�¼
    for i := FReRecvMessages.Count - 1 downto 0 do
    begin
      rMsg := FReRecvMessages.Items[0];
      if rMsg.id = msg_id then
      begin
        OutputDebugString(PWideChar(IntToStr(FReRecvMessages.Count) + 'Ignore duplicate message:' + msg.ToJSONString));
        Result := True; // ��Ϣ�Ѿ����ڣ�ȥ��
      end;
      // ����10���ӵ������
      if GetTickCount() - rMsg.tick > 600 * 1000 then
        FReRecvMessages.Remove(rMsg);
    end;
    if Result then
      exit();
    // �ս��յ�����Ϣ����ôҪ��¼���ҷ��ظ��߳�����Ҫ����
    Result := False;
    rMsg.id := msg_id;
    rMsg.tick := GetTickCount();
    FReRecvMessages.Add(rMsg);
    OutputDebugString(PWideChar(IntToStr(FReRecvMessages.Count) + 'EnQueue Msg for De-duplicate received: ' +
      msg.ToJSONString + ' ' + msg_id));
  finally
    TMonitor.exit(FReRecvMessages)
  end;
end;

initialization

cmd_seq_obj := TSequenceManager.Create;

finalization

cmd_seq_obj.Free;
cmd_seq_obj := nil;
if Assigned(SingletonNATSObject) then
begin
  SingletonNATSObject.Terminate;
  FreeAndNil(SingletonNATSObject);
end;

end.
