package cmd

import (
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math/big"
	"memoc/contracts/erc20"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethclient"
	mapi "github.com/memoio/go-mefs-v2/api"
	mclient "github.com/memoio/go-mefs-v2/api/client"
	"github.com/memoio/go-mefs-v2/lib/address"
	metag "github.com/memoio/go-mefs-v2/lib/utils/etag"
	minio "github.com/memoio/minio/cmd"
	"github.com/memoio/relay/lib/utils"
	"github.com/minio/cli"
	"github.com/minio/madmin-go"
	miniogo "github.com/minio/minio-go/v7"
	"github.com/minio/pkg/bucket/policy"
	"github.com/minio/pkg/bucket/policy/condition"
	"github.com/minio/pkg/mimedb"
	"github.com/mitchellh/go-homedir"
	cli2 "github.com/urfave/cli/v2"
	"golang.org/x/crypto/sha3"
	"golang.org/x/xerrors"
)

var (
	ENDPOINT         string
	GatewayAddr      = common.HexToAddress("0x31e7829Ea2054fDF4BCB921eDD3a98a825242267")
	GatewaySecretKey = "8a87053d296a0f0b4600173773c8081b12917cef7419b2675943b0aa99429b62"
)

var GatewayRunCmd = &cli2.Command{
	Name:  "run",
	Usage: "run a memo gateway",
	Flags: []cli2.Flag{
		&cli2.StringFlag{
			Name:    "username",
			Aliases: []string{"n"},
			Usage:   "input your user name",
			Value:   "memo",
		},
		&cli2.StringFlag{
			Name:    "password",
			Aliases: []string{"p"},
			Usage:   "input your password",
			Value:   "memoriae",
		},
		&cli2.StringFlag{
			Name:    "endpoint",
			Aliases: []string{"e"},
			Usage:   "input your endpoint",
			Value:   "0.0.0.0:5080",
		},
		&cli2.StringFlag{
			Name:    "console",
			Aliases: []string{"c"},
			Usage:   "input your console for browser",
			Value:   "8080",
		},
		&cli2.StringFlag{
			Name:    "memoep",
			Aliases: []string{"m"},
			Usage:   "memo chain endpoint",
			Value:   "https://chain.metamemo.one:8501",
		},
	},
	Action: func(cctx *cli2.Context) error {
		var terminate = make(chan os.Signal, 1)
		signal.Notify(terminate, os.Interrupt, syscall.SIGTERM)
		defer signal.Stop(terminate)

		username := cctx.String("username")
		if username == "" {
			return xerrors.New("username is nil")
		}

		pwd := cctx.String("password")
		if pwd == "" {
			return xerrors.New("username is nil")
		}
		endPoint := cctx.String("endpoint")
		consoleAddress := cctx.String("console")
		if !strings.Contains(consoleAddress, ":") {
			consoleAddress = ":" + consoleAddress
		}

		ENDPOINT = cctx.String("memoep")

		// save process id
		pidpath, err := BestKnownPath()
		if err != nil {
			return err
		}
		pid := os.Getpid()
		pids := []byte(strconv.Itoa(pid))
		err = os.WriteFile(path.Join(pidpath, "pid"), pids, 0644)
		if err != nil {
			return err
		}

		err = Start(username, pwd, endPoint, consoleAddress)
		if err != nil {
			return err
		}

		<-terminate
		log.Println("received shutdown signal")
		log.Println("shutdown...")

		return nil
	},
}

var GatewayStopCmd = &cli2.Command{
	Name:  "stop",
	Usage: "stop a memo gateway",
	Action: func(_ *cli2.Context) error {
		pidpath, err := BestKnownPath()
		if err != nil {
			return err
		}

		pd, _ := ioutil.ReadFile(path.Join(pidpath, "pid"))

		err = kill(string(pd))
		if err != nil {
			return err
		}
		log.Println("gateway gracefully exit...")

		return nil
	},
}

var DefaultPathRoot string = "~/.mefs_gw"

func BestKnownPath() (string, error) {
	mefsPath := DefaultPathRoot
	mefsPath, err := homedir.Expand(mefsPath)
	if err != nil {
		return "", err
	}

	_, err = os.Stat(mefsPath)
	if os.IsNotExist(err) {
		err = os.Mkdir(mefsPath, 0755)
		if err != nil {
			return "", err
		}
	}
	return mefsPath, nil
}

func kill(pid string) error {
	switch runtime.GOOS {
	case "linux":
		return exec.Command("kill", "-15", pid).Run()
	case "windows":
		return exec.Command("taskkill", "/F", "/T", "/PID", pid).Run()
	default:
		return fmt.Errorf("unsupported platform %s", runtime.GOOS)
	}
}

func Start(addr, pwd, endPoint, consoleAddress string) error {
	minio.RegisterGatewayCommand(cli.Command{
		Name:            "lfs",
		Usage:           "Mefs Log File System Service (LFS)",
		Action:          mefsGatewayMain,
		HideHelpCommand: true,
	})
	err := os.Setenv("MINIO_ROOT_USER", addr)
	if err != nil {
		return err
	}
	err = os.Setenv("MINIO_ROOT_PASSWORD", pwd)
	if err != nil {
		return err
	}

	rootpath, err := BestKnownPath()
	if err != nil {
		return err
	}

	gwConf := rootpath + "/gwConf"

	// ”memoriae“ is app name
	// "gateway" represents gatewat mode; respective, "server" represents server mode
	// "lfs" is subcommand, should equal to RegisterGatewayCommand{Name}
	go minio.Main([]string{"memoriae", "gateway", "lfs",
		"--address", endPoint, "--config-dir", gwConf, "--console-address", consoleAddress})

	return nil
}

// Handler for 'minio gateway oss' command line.
func mefsGatewayMain(ctx *cli.Context) {
	minio.StartGateway(ctx, &Mefs{"lfs"})
}

type Mefs struct {
	host string
}

// Name implements Gateway interface.
func (g *Mefs) Name() string {
	return "mefs"
}

// NewGatewayLayer implements Gateway interface and returns LFS ObjectLayer.
func (g *Mefs) NewGatewayLayer(creds madmin.Credentials) (minio.ObjectLayer, error) {
	gw := &lfsGateway{
		polices: make(map[string]*policy.Policy),
	}

	return gw, nil
}

// Production - oss is production ready.
func (g *Mefs) Production() bool {
	return false
}

// lfsGateway implements gateway.
type lfsGateway struct {
	minio.GatewayUnsupported
	memofs  *MemoFs
	polices map[string]*policy.Policy
}

// Shutdown saves any gateway metadata to disk
// if necessary and reload upon next restart.
func (l *lfsGateway) Shutdown(ctx context.Context) error {
	return nil
}

func (l *lfsGateway) IsEncryptionSupported() bool {
	return true
}

func (l *lfsGateway) QueryPrice(ctx context.Context, bucket, size, time string) (string, error) {
	err := l.getMemofs()
	if err != nil {
		return "", minio.Memo{Message: fmt.Sprintf("connect error, %v", err)}
	}
	if time == "" {
		time = "365"
	}
	price, err := l.memofs.QueryPrice(ctx)
	if err != nil {
		return "", minio.Memo{Message: fmt.Sprintf("gateway get price error, %v", err)}
	}
	log.Println("QueryPrice", price, bucket, size, time)
	pr := new(big.Int)
	pr.SetString(price, 10)

	ssize := new(big.Int)
	ssize.SetString(size, 10)

	stime := new(big.Int)
	stime.SetString(time, 10)
	stime.Mul(stime, big.NewInt(86400))

	if stime.Cmp(big.NewInt(86400)) < 0 {
		return "", minio.Memo{Message: fmt.Sprintf("at least storage 100 days")}
	}

	bi, err := l.memofs.GetBucketInfo(ctx, bucket)
	if err != nil {
		return "", minio.Memo{Message: fmt.Sprintf("get bucket info error %v", err)}
	}

	segment := new(big.Int)
	segment.Mul(ssize, big.NewInt(int64(bi.DataCount+bi.ParityCount)))

	amount := new(big.Int)
	amount.Mul(pr, stime)
	amount.Mul(amount, segment)
	amount.Div(amount, big.NewInt(248000))
	amount.Div(amount, big.NewInt(int64(bi.DataCount)))

	return amount.String(), nil
}

func (l *lfsGateway) StorageInfo(ctx context.Context) (si minio.StorageInfo, errs []error) {
	//log.Println("get StorageInfo")
	si.Backend.Type = madmin.Gateway

	_, closer, err := mclient.NewUserNode(ctx, l.memofs.addr, l.memofs.headers)
	if err == nil {
		closer()
		si.Backend.GatewayOnline = true
	}

	return si, nil
}

// MakeBucketWithLocation creates a new container on LFS backend.
func (l *lfsGateway) MakeBucketWithLocation(ctx context.Context, bucket string, options minio.BucketOptions) error {
	// err := l.getMemofs()
	// if err != nil {
	// 	return err
	// }
	// err = l.memofs.MakeBucketWithLocation(ctx, bucket)
	// if err != nil {
	// 	return err
	// }
	return minio.NotImplemented{}
}

// SetBucketPolicy will set policy on bucket.
func (l *lfsGateway) SetBucketPolicy(ctx context.Context, bucket string, bucketPolicy *policy.Policy) error {
	_, err := l.GetBucketInfo(ctx, bucket)
	if err != nil {
		return err
	}
	l.polices[bucket] = bucketPolicy
	return nil
}

// GetBucketPolicy will get policy on bucket.
func (l *lfsGateway) GetBucketPolicy(ctx context.Context, bucket string) (*policy.Policy, error) {
	if bucket == "favicon.ico" {
		return &policy.Policy{}, nil
	}
	err := l.getMemofs()
	if err != nil {
		return nil, err
	}

	bi, err := l.memofs.GetBucketInfo(ctx, bucket)
	if err != nil {
		return nil, err
	}

	pb, ok := l.polices[bucket]
	if ok {
		return pb, nil
	}

	pp := &policy.Policy{
		ID:      policy.ID(fmt.Sprintf("data: %d, parity: %d", bi.DataCount, bi.ParityCount)),
		Version: policy.DefaultVersion,
		Statements: []policy.Statement{
			policy.NewStatement(
				"",
				policy.Allow,

				policy.NewPrincipal("*"),
				policy.NewActionSet(
					policy.GetObjectAction,
					policy.PutObjectAction,

					policy.ListBucketAction,
				),
				policy.NewResourceSet(
					policy.NewResource(bucket, ""),
					policy.NewResource(bucket, "*"),
				),
				condition.NewFunctions(),
			),
		},
	}

	return pp, nil
}

// GetBucketInfo gets bucket metadata.
func (l *lfsGateway) GetBucketInfo(ctx context.Context, bucket string) (bi minio.BucketInfo, err error) {
	//log.Println("get buckte info: ", bucket)
	err = l.getMemofs()
	if err != nil {
		return bi, err
	}
	bucketInfo, err := l.memofs.GetBucketInfo(ctx, bucket)
	if err != nil {
		return bi, err
	}
	bi.Name = bucket
	bi.Created = time.Unix(bucketInfo.GetCTime(), 0).UTC()
	return bi, nil
}

// ListBuckets lists all LFS buckets.
func (l *lfsGateway) ListBuckets(ctx context.Context) (bs []minio.BucketInfo, err error) {
	log.Println("list bucktes")
	// err = l.getMemofs()
	// if err != nil {
	// 	return bs, err
	// }
	// buckets, err := l.memofs.ListBuckets(ctx)
	// if err != nil {
	// 	return nil, err
	// }

	// bs = make([]minio.BucketInfo, 0, len(buckets))
	// for _, v := range buckets {
	// 	bs = append(bs, minio.BucketInfo{
	// 		Name:    v.Name,
	// 		Created: time.Unix(v.GetCTime(), 0).UTC(),
	// 	})
	// }
	// return bs, nil
	return bs, minio.NotImplemented{}
}

// DeleteBucket deletes a bucket on LFS.
func (l *lfsGateway) DeleteBucket(ctx context.Context, bucket string, opts minio.DeleteBucketOptions) error {
	return minio.NotImplemented{}
}

// ListObjects lists all blobs in LFS bucket filtered by prefix.
func (l *lfsGateway) ListObjects(ctx context.Context, bucket, prefix, marker, delimiter string, maxKeys int) (loi minio.ListObjectsInfo, err error) {
	//log.Println("list object: ", bucket, prefix, marker, delimiter, maxKeys)
	err = l.getMemofs()
	if err != nil {
		return loi, err
	}
	mloi, err := l.memofs.ListObjects(ctx, bucket, prefix, marker, delimiter, maxKeys)
	if err != nil {
		return loi, err
	}

	for _, oi := range mloi.Objects {
		ud := make(map[string]string)
		if oi.UserDefined != nil {
			ud = oi.UserDefined
		}
		//  for s3fs
		ud["x-amz-meta-mode"] = "33204"
		ud["x-amz-meta-mtime"] = time.Unix(oi.GetTime(), 0).Format(utils.SHOWTIME)
		ud["x-amz-meta-state"] = oi.State
		etag, _ := metag.ToString(oi.ETag)
		loi.Objects = append(loi.Objects, minio.ObjectInfo{
			Bucket:      bucket,
			Name:        oi.GetName(),
			ModTime:     time.Unix(oi.GetTime(), 0).UTC(),
			Size:        int64(oi.Size),
			IsDir:       false,
			ETag:        etag,
			UserDefined: ud,
		})
	}

	loi.IsTruncated = mloi.IsTruncated
	loi.NextMarker = mloi.NextMarker
	loi.Prefixes = mloi.Prefixes

	return loi, nil
}

// ListObjectsV2 lists all blobs in LFS bucket filtered by prefix
func (l *lfsGateway) ListObjectsV2(ctx context.Context, bucket, prefix, continuationToken, delimiter string, maxKeys int,
	fetchOwner bool, startAfter string) (loiv2 minio.ListObjectsV2Info, err error) {
	//log.Println("list objects v2: ", bucket, prefix, continuationToken, delimiter, maxKeys, startAfter)
	marker := continuationToken
	if marker == "" {
		marker = startAfter
	}

	loi, err := l.ListObjects(ctx, bucket, prefix, marker, delimiter, maxKeys)
	if err != nil {
		return loiv2, err
	}

	loiv2 = minio.ListObjectsV2Info{
		IsTruncated:           loi.IsTruncated,
		ContinuationToken:     continuationToken,
		NextContinuationToken: loi.NextMarker,
		Objects:               loi.Objects,
		Prefixes:              loi.Prefixes,
	}

	return loiv2, err
}

// GetObjectNInfo - returns object info and locked object ReadCloser
func (l *lfsGateway) GetObjectNInfo(ctx context.Context, bucket, object string, rs *minio.HTTPRangeSpec, h http.Header, lockType minio.LockType, opts minio.ObjectOptions) (gr *minio.GetObjectReader, err error) {
	//log.Println("get objectn: ", bucket, object)
	objInfo, err := l.GetObjectInfo(ctx, bucket, object, opts)
	if err != nil {
		return nil, minio.ErrorRespToObjectError(err, bucket, object)
	}
	if objInfo.UserDefined["content-type"] == "" {
		objInfo.UserDefined["content-type"] = mimedb.TypeByExtension(path.Ext(object))
	}
	fn, off, length, err := minio.NewGetObjectReader(rs, objInfo, opts)
	if err != nil {
		return nil, minio.ErrorRespToObjectError(err, bucket, object)
	}

	pr, pw := io.Pipe()
	go func() {
		err := l.GetObject(ctx, bucket, object, off, length, pw, objInfo.ETag, opts)
		pw.CloseWithError(err)
	}()

	// Setup cleanup function to cause the above go-routine to
	// exit in case of partial read
	pipeCloser := func() { pr.Close() }
	return fn(pr, h, pipeCloser)
}

// GetObjectInfo reads object info and replies back ObjectInfo.
func (l *lfsGateway) GetObjectInfo(ctx context.Context, bucket, object string, opts minio.ObjectOptions) (objInfo minio.ObjectInfo, err error) {
	err = l.getMemofs()
	if err != nil {
		return objInfo, err
	}
	moi, err := l.memofs.GetObjectInfo(ctx, bucket, object)
	if err != nil {
		return objInfo, err
	}

	ud := make(map[string]string)
	if moi.UserDefined != nil {
		ud = moi.UserDefined
	}
	// for s3fs
	ud["x-amz-meta-mode"] = "33204"
	ud["x-amz-meta-mtime"] = time.Unix(moi.GetTime(), 0).Format(utils.SHOWTIME)
	ud["x-amz-meta-state"] = moi.State
	// need handle ETag
	etag, _ := metag.ToString(moi.ETag)
	oi := minio.ObjectInfo{
		Bucket:      bucket,
		Name:        moi.Name,
		ModTime:     time.Unix(moi.GetTime(), 0),
		Size:        int64(moi.Size),
		ETag:        etag,
		IsDir:       false,
		UserDefined: ud,
	}

	return oi, nil
}

// GetObject reads an object on LFS. Supports additional
// parameters like offset and length which are synonymous with
// HTTP Range requests.
//
// startOffset indicates the starting read location of the object.
// length indicates the total length of the object.
func (l *lfsGateway) GetObject(ctx context.Context, bucketName, objectName string, startOffset, length int64, writer io.Writer, etag string, o minio.ObjectOptions) error {
	err := l.getMemofs()
	if err != nil {
		return err
	}
	err = l.memofs.GetObject(ctx, bucketName, objectName, startOffset, length, writer)
	if err != nil {
		return err
	}
	return nil
}

// PutObject creates a new object with the incoming data.
func (l *lfsGateway) PutObject(ctx context.Context, bucket, object string, r *minio.PutObjReader, opts minio.ObjectOptions) (objInfo minio.ObjectInfo, err error) {
	err = l.getMemofs()
	if err != nil {
		return objInfo, err
	}

	signmsg := opts.UserDefined["X-Amz-Meta-Sign"]
	log.Println("signmsg", signmsg)
	date := opts.UserDefined["X-Amz-Meta-Date"]
	if date == "" {
		date = "365"
	}
	maddr := common.HexToAddress(strings.ToLower(bucket))
	datebyte := crypto.Keccak256([]byte(date))
	log.Printf("datahash %x", datebyte)
	sig := hexutil.MustDecode(string(signmsg))
	if sig[64] == 27 || sig[64] == 28 {
		sig[64] -= 27
	}

	if !l.validAddress(ctx, maddr, datebyte, sig) {
		return objInfo, minio.SignNotRight{}
	}

	putOpts := miniogo.PutObjectOptions{
		UserMetadata:         opts.UserDefined,
		ServerSideEncryption: opts.ServerSideEncryption,
		SendContentMd5:       true,
	}

	_, err = l.memofs.GetObjectInfo(ctx, bucket, object)
	if err == nil {
		mtime := time.Now().Format("20060102T150405")
		suffix := path.Ext(object)
		if len(suffix) > 0 && len(object) > len(suffix) {
			object = object[:len(object)-len(suffix)] + "-" + mtime + suffix
		} else {
			object = object + "-" + mtime
		}
	}

	contentType := putOpts.ContentType
	if contentType == "" {
		contentType = "application/octet-stream"
	}
	if opts.UserDefined != nil {
		opts.UserDefined["Content-Type"] = contentType
	}
	moi, err := l.memofs.PutObject(ctx, bucket, object, r, opts.UserDefined)
	if err != nil {
		return objInfo, minio.Memo{Message: fmt.Sprintf("Gateway putobject error %s", err)}
	}

	etag, _ := metag.ToString(moi.ETag)
	size := big.NewInt(int64(moi.Size))
	price, err := l.QueryPrice(ctx, bucket, size.String(), date)
	if err != nil {
		l.memofs.DeleteObject(ctx, bucket, object)
		return objInfo, err
	}
	pri := new(big.Int)
	pri.SetString(price, 10)

	balance, err := l.GetBalanceInfo(ctx, bucket)
	if err != nil {
		l.memofs.DeleteObject(ctx, bucket, object)
		return objInfo, err
	}
	log.Println("Price", price)
	log.Println("Balance", balance)
	bal := new(big.Int)
	bal.SetString(balance, 10)
	if bal.Cmp(pri) < 0 {
		log.Printf("allow: %d, price: %d, allowance not enough\n", bal, pri)
		l.memofs.DeleteObject(ctx, bucket, object)
		return objInfo, minio.BalanceNotEnough{}
	}

	to := common.HexToAddress(bucket)

	if !l.pay(ctx, to, etag, pri, size) {
		log.Printf("pay error")
		l.memofs.DeleteObject(ctx, bucket, object)
		return objInfo, minio.PayNotComplete{}
	}
	log.Println("complete upload ", bucket, object)

	oi := minio.ObjectInfo{
		Bucket:  bucket,
		Name:    moi.Name,
		ModTime: time.Unix(moi.GetTime(), 0),
		Size:    int64(moi.Size),
		ETag:    etag,
		IsDir:   false,
	}

	if moi.UserDefined != nil {
		oi.UserDefined = moi.UserDefined
		oi.UserDefined["x-amz-meta-state"] = moi.State
		oi.UserDefined["x-amz-meta-date"] = date
		oi.UserDefined["x-memo-price"] = price
	}

	return oi, nil
}

// DeleteObject deletes a blob in bucket.
func (l *lfsGateway) DeleteObject(ctx context.Context, bucket, object string, opts minio.ObjectOptions) (minio.ObjectInfo, error) {
	return minio.ObjectInfo{}, minio.NotImplemented{}
}

func (l *lfsGateway) DeleteObjects(ctx context.Context, bucket string, objects []minio.ObjectToDelete, opts minio.ObjectOptions) ([]minio.DeletedObject, []error) {
	errs := make([]error, len(objects))
	dobjects := make([]minio.DeletedObject, len(objects))
	for idx, object := range objects {
		_, errs[idx] = l.DeleteObject(ctx, bucket, object.ObjectName, opts)
		if errs[idx] == nil {
			dobjects[idx] = minio.DeletedObject{
				ObjectName: object.ObjectName,
			}
		}
	}

	return dobjects, errs
}

func (l lfsGateway) GetTokenAddress(ctx context.Context) (string, error) {
	// gi, err := l.getGroupInfo(ctx)
	// if err != nil {
	// 	return "", err
	// }

	// client, err := ethclient.DialContext(ctx, gi.EndPoint)
	// if err != nil {
	// 	return "", err
	// }
	// instanceAddr := common.HexToAddress(gi.BaseAddr)

	// instanceIns, _ := inst.NewInstance(instanceAddr, client)
	// addr, err := instanceIns.Instances(&bind.CallOpts{From: com.AdminAddr}, com.TypeERC20)
	// if err != nil {
	// 	return "", err
	// }
	// return addr.Hex(), nil
	return "", minio.NotImplemented{}
}

func (l lfsGateway) Approve(ctx context.Context, ts, faddr string) error {
	// log.Println("approve: ", ts)
	// err := l.getMemofs()
	// if err != nil {
	// 	return err
	// }
	// tsbyte, err := hex.DecodeString(ts)
	// if err != nil {
	// 	return err
	// }
	// tsa := new(types.Transaction)
	// tsa.UnmarshalBinary(tsbyte)

	// result := l.sendTransaction(ctx, tsa, "approve")
	// if !result {
	// 	return xerrors.New("transaction error")
	// }
	return minio.NotImplemented{}
}

func (l lfsGateway) GetGatewayAddress(ctx context.Context) (string, error) {
	// err := l.getMemofs()
	// if err != nil {
	// 	return "", err
	// }

	// addr, err := l.memofs.GetWalletAddress(ctx)
	// if err != nil {
	// 	return "", err
	// }

	// return hex.EncodeToString(addr), nil
	return "", minio.NotImplemented{}
}

// func (l *lfsGateway) GetBalanceInfo(ctx context.Context, address string) (string, error) {
// 	err := l.getMemofs()
// 	if err != nil {
// 		return "", err
// 	}
// 	// if bucket not exist, create bucket
// 	log.Println("GetBalanceInfo ", address)
// 	err = l.memofs.MakeBucketWithLocation(ctx, address)
// 	if err != nil {
// 		err1 := minio.BucketAlreadyExists{Bucket: address, Object: ""}
// 		if err.Error() != err1.Error() {
// 			return "", err
// 		}
// 	} else {
// 		time.Sleep(20 * time.Second)
// 	}

// 	addr := common.HexToAddress(address)
// 	instanceAddr, endPoint := com.GetInsEndPointByChain("product")
// 	client, err := ethclient.DialContext(ctx, endPoint)
// 	if err != nil {
// 		return "", err
// 	}

// 	instanceIns, _ := inst.NewInstance(instanceAddr, client)
// 	erc20Addr, err := instanceIns.Instances(&bind.CallOpts{From: com.AdminAddr}, com.TypeERC20)
// 	if err != nil {
// 		return "", err
// 	}

// 	erc20Ins, err := erc.NewERC20(erc20Addr, client)
// 	if err != nil {
// 		return "", err
// 	}

// 	bal, err := erc20Ins.BalanceOf(&bind.CallOpts{From: com.AdminAddr}, addr)
// 	if err != nil {
// 		return "", err
// 	}

// 	return bal.String(), nil
// }

func (l *lfsGateway) GetBalanceInfo(ctx context.Context, address string) (string, error) {
	err := l.getMemofs()
	if err != nil {
		return "", err
	}
	// if bucket not exist, create bucket
	log.Println("GetBalanceInfo ", address)
	err = l.memofs.MakeBucketWithLocation(ctx, address)
	if err != nil {
		err1 := minio.BucketAlreadyExists{Bucket: address, Object: ""}
		if err.Error() != err1.Error() {
			return "", minio.Memo{Message: fmt.Sprintf("create bucket error %s", err)}
		}
	} else {
		log.Println("create bucket ", address)
		time.Sleep(20 * time.Second)
	}

	client, err := ethclient.DialContext(ctx, ENDPOINT)
	if err != nil {
		log.Println("connect to eth error", err)
		return "", err
	}

	defer client.Close()

	addr := common.HexToAddress(address)
	contractAddr := common.HexToAddress("0xCcf7b7F747100f3393a75DDf6864589f76F4eA25")

	balanceOfFnSignature := []byte("balanceOf(address)")
	hash := sha3.NewLegacyKeccak256()
	hash.Write(balanceOfFnSignature)
	methodID := hash.Sum(nil)[:4]

	paddedAddress := common.LeftPadBytes(addr.Bytes(), 32)

	var data []byte
	data = append(data, methodID...)
	data = append(data, paddedAddress...)

	msg := ethereum.CallMsg{
		To:   &contractAddr,
		Data: data,
	}

	result, err := client.CallContract(ctx, msg, nil)
	if err != nil {
		return "", err
	}
	bal := new(big.Int)
	bal.SetBytes(result)
	log.Printf("address: %s,balance: %s\n", addr, bal)

	return bal.String(), nil
}

const transferTopic = "0x8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925"
const approveTopic = "0x8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925"
const payTopic = "0xc0e3b3bf3b856068b6537f07e399954cb5abc4fade906ee21432a8ded3c36ec8"

func (l *lfsGateway) getGroupInfo(ctx context.Context) (*mapi.GroupInfo, error) {
	err := l.getMemofs()
	if err != nil {
		return nil, err
	}
	return l.memofs.GetGroupInfo(ctx)
}

func (l *lfsGateway) sendTransaction(ctx context.Context, signedTx *types.Transaction, ttype string) bool {
	log.Println("sendTransaction")
	client, err := ethclient.Dial(ENDPOINT)
	if err != nil {
		log.Println(err)
		return false
	}
	defer client.Close()

	err = client.SendTransaction(ctx, signedTx)
	if err != nil {
		log.Println(err)
		return false
	}

	log.Println("waiting tx complete...")
	time.Sleep(30 * time.Second)

	receipt, err := client.TransactionReceipt(ctx, signedTx.Hash())
	if err != nil {
		log.Println(err)
		return false
	}
	if receipt.Status != 1 {
		log.Println("Status not right")
		log.Println(receipt.Logs)
		log.Println(receipt)
		return false
	}

	if len(receipt.Logs) == 0 {
		log.Println("no logs")
		return false
	}

	if len(receipt.Logs[0].Topics) == 0 {
		log.Println("no topics")
		return false
	}

	var topic string
	switch ttype {
	case "transfer":
		topic = transferTopic
	case "approve":
		topic = approveTopic
	case "pay":
		topic = payTopic
	}

	if receipt.Logs[0].Topics[0].String() != topic {
		log.Println("topic not right: ", receipt.Logs[0].Topics[0].String())
		return false
	}

	return true
}

func (l *lfsGateway) getAllowance(ctx context.Context, sender common.Address) (*big.Int, error) {
	addr, err := l.memofs.GetWalletAddress(ctx)
	if err != nil {
		return nil, err
	}

	reciver := common.BytesToAddress(addr)

	var allowance *big.Int

	client, err := ethclient.Dial(ENDPOINT)
	if err != nil {
		log.Println(err)
		return nil, err
	}
	defer client.Close()

	taddr, err := l.GetTokenAddress(ctx)
	if err != nil {
		return nil, err
	}
	tokenaddress := common.HexToAddress(taddr)
	erc20Ins, err := erc20.NewERC20(tokenaddress, client)
	if err != nil {
		return nil, err
	}

	allowance, err = erc20Ins.Allowance(&bind.CallOpts{
		From: reciver,
	}, sender, reciver)
	if err != nil {
		return nil, err
	}
	log.Println("allowance: ", allowance)
	return allowance, nil
}

func (l *lfsGateway) transfer(ctx context.Context, sender common.Address, value *big.Int) (*types.Transaction, error) {
	addr, err := l.GetGatewayAddress(ctx)
	if err != nil {
		return nil, err
	}
	maddr := common.HexToAddress(addr)
	if err != nil {
		return nil, err
	}

	useraddr, err := address.NewAddress(maddr.Bytes())
	if err != nil {
		log.Println("get addr error")
		return nil, err
	}

	log.Println(useraddr)
	sks, err := l.memofs.WalletExport(ctx, useraddr)
	if err != nil {
		log.Println("wallet export error", err)
		return nil, err
	}

	sk := hex.EncodeToString(sks.SecretKey)
	reciver := common.HexToAddress(addr)

	client, err := ethclient.Dial(ENDPOINT)
	if err != nil {
		return nil, err
	}
	defer client.Close()

	nonce, err := client.PendingNonceAt(ctx, reciver)
	if err != nil {
		return nil, err
	}

	chainID, err := client.NetworkID(ctx)
	if err != nil {
		log.Println(err)
		return nil, err
	}
	log.Println("chainID: ", chainID)

	taddr, err := l.GetTokenAddress(ctx)
	if err != nil {
		return nil, err
	}
	tokenaddress := common.HexToAddress(taddr)

	transferFnSignature := []byte("transferFrom(address,address,uint256)")

	hash := sha3.NewLegacyKeccak256()
	hash.Write(transferFnSignature)
	methodID := hash.Sum(nil)[:4]

	paddedFromAddress := common.LeftPadBytes(sender.Bytes(), 32)
	paddedAddress := common.LeftPadBytes(reciver.Bytes(), 32)

	paddedAmount := common.LeftPadBytes(value.Bytes(), 32)

	var data []byte
	data = append(data, methodID...)
	data = append(data, paddedFromAddress...)
	data = append(data, paddedAddress...)
	data = append(data, paddedAmount...)

	gasLimit := uint64(300000)
	gasPrice := big.NewInt(1000)

	tx := types.NewTransaction(nonce, tokenaddress, big.NewInt(0), gasLimit, gasPrice, data)
	privateKey, err := crypto.HexToECDSA(sk)
	if err != nil {
		return nil, err
	}

	signedTx, err := types.SignTx(tx, types.NewEIP155Signer(chainID), privateKey)
	if err != nil {
		return nil, err
	}

	return signedTx, nil
}

func (l *lfsGateway) pay(ctx context.Context, to common.Address, hashid string, amount *big.Int, size *big.Int) bool {
	client, err := ethclient.Dial(ENDPOINT)
	if err != nil {
		return false
	}
	defer client.Close()

	nonce, err := client.PendingNonceAt(ctx, GatewayAddr)
	if err != nil {
		return false
	}
	log.Println("nonce: ", nonce)

	chainID, err := client.NetworkID(ctx)
	if err != nil {
		log.Println(err)
		return false
	}
	log.Println("chainID: ", chainID)

	toaddress := common.HexToAddress("0xCcf7b7F747100f3393a75DDf6864589f76F4eA25")
	storeOrderPayFnSignature := []byte("storeOrderpay(address,string,uint256,uint256)")
	hash := sha3.NewLegacyKeccak256()
	hash.Write(storeOrderPayFnSignature)
	methodID := hash.Sum(nil)[:4]

	paddedAddress := common.LeftPadBytes(to.Bytes(), 32)
	paddedHashLen := common.LeftPadBytes(big.NewInt(int64(len([]byte(hashid)))).Bytes(), 32)
	paddedHashOffset := common.LeftPadBytes(big.NewInt(32*4).Bytes(), 32)
	paddedMd5 := common.LeftPadBytes([]byte(hashid), 32)
	paddedAmount := common.LeftPadBytes(amount.Bytes(), 32)
	PaddedSize := common.LeftPadBytes(size.Bytes(), 32)

	var data []byte
	data = append(data, methodID...)
	data = append(data, paddedAddress...)
	data = append(data, paddedHashOffset...)
	data = append(data, paddedAmount...)
	data = append(data, PaddedSize...)
	data = append(data, paddedHashLen...)
	data = append(data, paddedMd5...)

	gasLimit := uint64(300000)
	gasPrice := big.NewInt(1000)
	tx := types.NewTransaction(nonce, toaddress, big.NewInt(0), gasLimit, gasPrice, data)

	privateKey, err := crypto.HexToECDSA(GatewaySecretKey)
	if err != nil {
		log.Println("get privateKey error: ", err)
		return false
	}

	signedTx, err := types.SignTx(tx, types.NewEIP155Signer(chainID), privateKey)
	if err != nil {
		log.Println("signedTx error: ", err)
		return false
	}

	return l.sendTransaction(ctx, signedTx, "pay")
}

func (l *lfsGateway) validAddress(ctx context.Context, to common.Address, date, sig []byte) bool {
	log.Printf("validAddress: %s %s, %x\n", to.Hex(), hexutil.Encode(sig), date)

	sigPublicKeyECDSA, err := crypto.SigToPub(date, sig)
	if err != nil {
		log.Println(err)
		return false
	}
	log.Println(crypto.PubkeyToAddress(*sigPublicKeyECDSA))
	sigPublicKeyBytes := crypto.FromECDSAPub(sigPublicKeyECDSA)

	if strings.Compare(to.Hex(), crypto.PubkeyToAddress(*sigPublicKeyECDSA).Hex()) != 0 {
		log.Println(to.Hex(), crypto.PubkeyToAddress(*sigPublicKeyECDSA).String())
		return false
	}

	ok := crypto.VerifySignature(sigPublicKeyBytes, []byte(date), sig[:len(sig)-1])

	if !ok {
		log.Println("sign not right")
		return false
	}
	return true
}
