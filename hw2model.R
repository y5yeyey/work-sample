####
## R version 3.3.1 (2016-06-21) -- "Bug in Your Hair"
## Platform: x86_64-apple-darwin13.4.0 (64-bit)
## Author: Congwei Wu
## ID: 22921487
## Date: Jan. 31, 2017
####

library(dplyr)


#### get leave-one-out MSPE estimate for any data.frame with a given model
get_MSPE_LeaveOneOut <- function(model, df) {
  helper <- function(i) {
    testdf <- df[i,]
    fit <- lm(model, data=df[-i, ])
    mean( (testdf[,1] - predict(fit, newdata=testdf))^2 )
  }
  mean(sapply(1:nrow(df), helper))
}

#### get regular Bootstrap MSPE estimate for any data.frame with a given model
get_MSPE_Bootstrap <- function(model, df, B=1000) {
  n <- nrow(df)
  helper <- function(i) {
    newdf <- df[sample(1:n, size=n, replace=TRUE), ]
    fit <- lm(model, data=newdf)
    mean( (df[,1] - predict(fit, newdata=newdf))^2 )
  }
  mean(sapply(1:B, helper))
}

#### get naive MSPE estimate for any data.frame with a given model
get_MSPE_Naive <- function(model, df) mean( (df[,1] - predict(lm(model, data=df), newdata=df))^2 )

#### get leave-one-out bootstrap MSPE estimate for any data.frame with a given model
get_MSPE_LeaveOneOutBootstrap <- function(model, df, B=1000) {
  n <- nrow(df)
  helper <- function(i) {
    keep <- sample(1:n, size=n, replace=TRUE)
    fit <- lm(model, data=df[keep,])
    newdf <- df[-keep,]
    ifelse( nrow(newdf) == 0, NA, mean( (newdf[,1] - predict(fit, newdata=newdf))^2 ) )
  }
  res <- mean(sapply(1:B, helper), na.rm=TRUE)
  ifelse(is.na(res), stop("Please increase your Bootstrap sample size! - C. Wu"), res)
}

#### get K-fold validation MSPE estimate for any data.frame with a given model
get_MSPE_KFoldValidation <- function(model, df, K, B=1000) {
  n <- nrow(df)
  K_samples <- c( rep(round(n/K), K-1), n - round(n/K) * (K-1) )
  K_start <- c(1)
  for (i in 2:K) K_start[i] <- K_start[i-1] + K_samples[i-1]
  K_end <- sapply(1:K, function(i) K_start[i] + K_samples[i] - 1)
  helper <- function(k){
    rows <- K_start[i]:K_end[i]
    testdf <- df[rows, ]
    fit <- lm(model, data=df[-rows, ])
    K_samples[k] * mean( (testdf[,1] - predict(fit, newdata=testdf))^2 )
  }
  mean( sapply( 1:B, function(i) sum(sapply(1:K, helper))) ) / n
}

#### get 0.632 weighted bootstrap estimator
get_MSPE_final <- function(MSPE_bootstrap, MSPE_one_out) .368*MSPE_bootstrap + .632*MSPE_one_out

#### display model results
show_Statistics <- function(model, df) {
  print(lm(model, df))
  aic <- AIC(lm(model, df))
  naive <- get_MSPE_Naive(model, df)
  bootstrap <- get_MSPE_Bootstrap(model, df)
  leave_one_out_bootstrap <- get_MSPE_LeaveOneOutBootstrap(model, df)
  weighted_bootstrap <- get_MSPE_final(bootstrap, leave_one_out_bootstrap)
  leave_one_out_validation <- get_MSPE_LeaveOneOut(model, df)
  k_fold_validation <- get_MSPE_KFoldValidation(model, df, 10)
  round(rbind(aic, naive, bootstrap, leave_one_out_bootstrap, weighted_bootstrap,
              leave_one_out_validation, k_fold_validation), 4)
}

#### obtain dataset
get_Data <- function() {
  df <- read.table("http://www.ics.uci.edu/~dgillen/STAT211/Data/pedgrowth1.txt", header=TRUE)
  df <- df[c("height", "id", "state", "age", "female", "racegrp", "duresrd", "gn")]
  regions <- list()
  for (state_name in unique(df$state)) {
    regions[state_name] <- ifelse( nrow(filter(df, state == state_name)) >= 20, state_name, "other" )
  }
  df$region <- sapply( 1:nrow(df), function(i) regions[[as.character(df[i,]$state)]] )
  df
}

#### obtain best model based on lowest AIC
get_Model_AIC <- function(df) step(lm(height ~ ., df), direction = "backward", trace=FALSE)

# > show_Statistics("height ~ age + female + racegrp + gn + duresrd", get_Data())
# [,1]
# aic                      9359.7671
# naive                     162.2247
# bootstrap                1353.2486
# leave_one_out_bootstrap   164.8982
# weighted_bootstrap        602.2111
# leave_one_out_validation  163.8823
# k_fold_validation         222.7275

# > show_Statistics("height ~ age + female + racegrp + region + duresrd + gn", get_Data())
# Call:
#   lm(formula = model, data = df)
# 
# Coefficients:
#   (Intercept)          age                  female                 racegrp           regionCalifornia  
# 77.3357                0.4186               -2.8754                1.4043               -2.0610  
# regionFlorida         regionGeorgia        regionIllinois       regionKentucky       regionLouisiana  
# -3.6983                3.2439               -3.5784               -1.2158               -0.7829  
# regionMaryland   regionMassachusetts       regionMichigan       regionMinnesota      regionMissouri  
# -2.9623                0.6973                3.9961                1.5533                2.0685  
# regionNew Jersey    regionNew York      regionNorth Carolina    regionOhio           regionOklahoma  
# -2.8081               -1.6568                0.7014                3.5698               -2.5464  
# regionother    regionPennsylvania  regionSouth Carolina           regionTexas        regionVirginia  
# -0.1129               -2.0348                1.7135               -2.3225               -5.6295  
# regionWashington   regionWisconsin          duresrd                  gn  
# -0.3561                0.7829               -0.1560                1.9654
# [,1]
# aic                      9366.3046
# naive                     156.8847
# bootstrap                1365.0789
# leave_one_out_bootstrap   168.3442
# weighted_bootstrap        608.7425
# leave_one_out_validation  164.7065
# k_fold_validation         225.2964

## my model
show_Statistics("height ~ age + female + racegrp + region + duresrd + gn", get_Data())

